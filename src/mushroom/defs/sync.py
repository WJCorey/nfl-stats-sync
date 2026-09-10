import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory

import dagster as dg

from mushroom.kernel import (
    KernelError,
    LocalArtifactStore,
    ScopePolicy,
    canonical_json,
    canonical_jsonl,
    parse_jsonl,
    reconcile,
)
from mushroom.current_state import capture_current_state
from mushroom.submission import SubmissionError, apply_plan, validate_plan
from project import acquisition, domain


def _store() -> LocalArtifactStore:
    return LocalArtifactStore(Path(os.environ.get("MUSHROOM_HOME", ".mushroom")))


@dg.resource(config_schema={"enabled": dg.Field(bool, default_value=False)})
def warmhub_client(context: dg.InitResourceContext) -> object | None:
    """An opt-in SDK client; no credentials are read while synchronization is off."""

    if not context.resource_config["enabled"]:
        return None
    from warmhub import WarmHubClient

    return WarmHubClient.from_env()


def _enabled_client(context: dg.AssetExecutionContext) -> object:
    client = context.resources.warmhub_client
    if client is None:
        raise dg.Failure("synchronize is disabled; enable the warmhub_client resource")
    return client


def _live_target() -> str:
    target = os.environ.get("MUSHROOM_TARGET")
    if not target or target == "example/mushroom-starter":
        raise dg.Failure("live jobs require an explicit non-starter MUSHROOM_TARGET")
    return target


def _policy(source: bytes) -> ScopePolicy:
    return ScopePolicy(
        target=domain.target(),
        pattern=domain.MANAGED_SCOPE,
        complete=json.loads(source)["complete"],
        absence=domain.ABSENCE_POLICY,
    )


def _capture_live_current_state(context: dg.AssetExecutionContext) -> tuple[int, bytes]:
    return capture_current_state(
        _enabled_client(context), target=_live_target(), pattern=domain.MANAGED_SCOPE
    )


def _plan_is_persisted(store: LocalArtifactStore) -> bool:
    try:
        _, manifest_payload = store.resolve("plan")
        operations_reference, _ = store.resolve("operations")
        manifest = json.loads(manifest_payload)
    except (KernelError, OSError, UnicodeDecodeError, json.JSONDecodeError):
        return False
    return isinstance(manifest, dict) and manifest.get("operations") == operations_reference.sha256


@dg.asset(config_schema={"live": dg.Field(bool, default_value=False)}, group_name="synchronization")
def source_snapshot(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Frozen, canonical source input selected by the project."""

    if context.op_execution_context.op_config["live"]:
        payload = acquisition.acquire()
        source = "public-sources"
    else:
        payload = canonical_json(json.loads(domain.source_fixture().read_bytes()))
        source = "fixture"
    store = _store()
    reference = store.persist(payload)
    store.publish("source", reference)
    store.publish("source-origin", store.persist(canonical_json({"source": source})))
    return dg.MaterializeResult(metadata={"sha256": reference.sha256, "bytes": reference.byte_length, "source": source})


@dg.asset(
    config_schema={"live": bool},
    group_name="synchronization",
    required_resource_keys={"warmhub_client"},
)
def current_state(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Fixture by default; a complete managed SDK read when explicitly enabled."""

    if context.op_execution_context.op_config["live"]:
        _, payload = _capture_live_current_state(context)
        source = "sdk-head"
    else:
        payload = canonical_jsonl(
            parse_jsonl(domain.current_state_fixture().read_bytes()), current=True
        )
        source = "fixture"
    store = _store()
    reference = store.persist(payload)
    store.publish("current", reference)
    store.publish("current-source", store.persist(canonical_json({"source": source})))
    return dg.MaterializeResult(metadata={"sha256": reference.sha256, "bytes": reference.byte_length})


@dg.asset(deps=[source_snapshot], group_name="synchronization")
def desired_state() -> dg.MaterializeResult:
    """Canonical operation-neutral records produced by project-owned code."""

    store = _store()
    source_reference, source = store.resolve("source")
    payload = canonical_jsonl(domain.transform(source))
    reference = store.persist(payload)
    store.publish("desired", reference)
    return dg.MaterializeResult(
        metadata={"sha256": reference.sha256, "bytes": reference.byte_length, "source_sha256": source_reference.sha256}
    )


@dg.asset(deps=[desired_state, current_state], group_name="synchronization")
def operation_plan() -> dg.MaterializeResult:
    """One deterministic persisted WarmHub operations.jsonl artifact."""

    store = _store()
    source_reference, source = store.resolve("source")
    desired_reference = store.reference("desired")
    current_reference = store.reference("current")
    _, current_source_payload = store.resolve("current-source")
    policy = _policy(source)
    with TemporaryDirectory(prefix="mushroom-plan-") as scratch:
        root = Path(scratch)
        desired_path, current_path, operations_path = root / "desired.jsonl", root / "current.jsonl", root / "operations.jsonl"
        store.copy(desired_reference, desired_path)
        store.copy(current_reference, current_path)
        result = reconcile(desired_path, current_path, operations_path, policy)
        payload = operations_path.read_bytes()
    summary = result.summary
    operations_reference = store.persist(payload)
    if operations_reference.sha256 != result.sha256 or operations_reference.byte_length != result.byte_length:
        raise RuntimeError("planner output identity changed during persistence")
    manifest = {
        "current": current_reference.sha256,
        "currentSource": json.loads(current_source_payload)["source"],
        "desired": desired_reference.sha256,
        "operations": operations_reference.sha256,
        "source": source_reference.sha256,
        "policy": {
            "absence": policy.absence,
            "complete": policy.complete,
            "managedScope": policy.pattern,
            "target": policy.target,
        },
        "summary": {
            "adds": summary.add_count,
            "operations": summary.operation_count,
            "preserved": summary.preserved_count,
            "retractions": summary.retract_count,
            "revisions": summary.revise_count,
            "unchanged": summary.unchanged_count,
        },
    }
    manifest_reference = store.persist(canonical_json(manifest))
    store.publish("operations", operations_reference)
    store.publish("plan", manifest_reference)
    return dg.MaterializeResult(metadata={"sha256": operations_reference.sha256, **manifest["summary"]})


@dg.asset_check(asset=operation_plan, blocking=True)
def operation_plan_is_persisted() -> dg.AssetCheckResult:
    """Fail closed unless the published plan and operation bytes agree."""

    store = _store()
    _, manifest_payload = store.resolve("plan")
    manifest = json.loads(manifest_payload)
    return dg.AssetCheckResult(
        passed=_plan_is_persisted(store), metadata={"operations": manifest["summary"]["operations"]}
    )


@dg.asset(
    deps=[operation_plan],
    group_name="synchronization",
    required_resource_keys={"warmhub_client"},
    retry_policy=dg.RetryPolicy(max_retries=0),
)
def server_validation(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Use WarmHub's non-writing evaluator on the exact checked plan."""

    store = _store()
    if not _plan_is_persisted(store):
        raise dg.Failure("operation plan is not persisted")
    operations_reference = store.reference("operations")
    plan_reference = store.reference("plan")
    _, manifest_payload = store.resolve("plan")
    manifest = json.loads(manifest_payload)
    if manifest.get("currentSource") != "sdk-head":
        raise dg.Failure("server validation requires a live current-state capture")
    with TemporaryDirectory(prefix="mushroom-validate-") as scratch:
        root = Path(scratch)
        operations_path, manifest_path = root / "operations.jsonl", root / "plan.json"
        store.copy(operations_reference, operations_path)
        store.copy(plan_reference, manifest_path)
        validated = validate_plan(
            _enabled_client(context),
            target=_live_target(),
            operations_path=operations_path,
            plan_sha256=manifest["operations"],
            plan_manifest_path=manifest_path,
        )
    return dg.MaterializeResult(
        metadata={
            "operations": validated.operation_count,
            "would_apply": validated.would_apply_count,
            "noop": validated.noop_count,
        }
    )


@dg.asset(
    deps=[operation_plan],
    group_name="synchronization",
    required_resource_keys={"warmhub_client"},
    retry_policy=dg.RetryPolicy(max_retries=0),
)
def synchronization_results(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Persist results from applying the exact checked plan once."""

    store = _store()
    if not _plan_is_persisted(store):
        raise dg.Failure("operation plan is not persisted")
    operations_reference = store.reference("operations")
    plan_reference = store.reference("plan")
    _, manifest_payload = store.resolve("plan")
    manifest = json.loads(manifest_payload)
    if manifest.get("currentSource") != "sdk-head":
        raise dg.Failure("synchronize requires a live current-state capture")
    with TemporaryDirectory(prefix="mushroom-submit-") as scratch:
        root = Path(scratch)
        operations_path, manifest_path, results_path = (
            root / "operations.jsonl",
            root / "plan.json",
            root / "results.jsonl",
        )
        store.copy(operations_reference, operations_path)
        store.copy(plan_reference, manifest_path)
        try:
            applied = apply_plan(
                _enabled_client(context),
                target=_live_target(),
                operations_path=operations_path,
                plan_sha256=manifest["operations"],
                plan_manifest_path=manifest_path,
                results_path=results_path,
            )
        except SubmissionError:
            try:
                failed_payload = results_path.read_bytes()
            except OSError:
                pass
            else:
                reference = store.persist(failed_payload)
                store.publish("results", reference)
            raise
        payload = results_path.read_bytes()
    reference = store.persist(payload)
    if reference.sha256 != applied.results_sha256:
        raise RuntimeError("submission results changed during persistence")
    store.publish("results", reference)
    return dg.MaterializeResult(
        metadata={
            "operations": applied.operation_count,
        }
    )


@dg.asset(
    deps=[synchronization_results],
    group_name="synchronization",
    required_resource_keys={"warmhub_client"},
)
def readback_state(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """A fresh complete managed SDK read after submission."""

    record_count, payload = _capture_live_current_state(context)
    store = _store()
    reference = store.persist(payload)
    store.publish("readback", reference)
    return dg.MaterializeResult(metadata={"records": record_count})


@dg.asset(deps=[desired_state, readback_state], group_name="synchronization")
def residual_plan() -> dg.MaterializeResult:
    """The zero-operation plan that proves the submitted state converged."""

    store = _store()
    source_reference, source = store.resolve("source")
    desired_reference = store.reference("desired")
    readback_reference = store.reference("readback")
    with TemporaryDirectory(prefix="mushroom-residual-") as scratch:
        root = Path(scratch)
        desired_path, current_path, operations_path = (
            root / "desired.jsonl",
            root / "current.jsonl",
            root / "operations.jsonl",
        )
        store.copy(desired_reference, desired_path)
        store.copy(readback_reference, current_path)
        result = reconcile(desired_path, current_path, operations_path, _policy(source))
        payload = operations_path.read_bytes()
    reference = store.persist(payload)
    if reference.sha256 != result.sha256 or reference.byte_length != result.byte_length:
        raise RuntimeError("residual plan changed during persistence")
    store.publish("residual", reference)
    return dg.MaterializeResult(metadata={"operations": result.summary.operation_count, "source_sha256": source_reference.sha256})


@dg.asset_check(asset=residual_plan, blocking=True)
def residual_plan_is_empty() -> dg.AssetCheckResult:
    """A successful synchronization has no residual operations."""

    _, payload = _store().resolve("residual")
    return dg.AssetCheckResult(passed=not payload, metadata={"operations": len(parse_jsonl(payload))})


prepare_sync = dg.define_asset_job(
    "prepare_sync",
    selection=dg.AssetSelection.assets(operation_plan).upstream()
    | dg.AssetSelection.checks_for_assets(operation_plan),
    config={"ops": {"current_state": {"config": {"live": False}}}},
    description="Prepare and check one persisted plan without WarmHub writes.",
)

prepare_fresh_sync = dg.define_asset_job(
    "prepare_fresh_sync",
    selection=dg.AssetSelection.assets(operation_plan).upstream()
    | dg.AssetSelection.checks_for_assets(operation_plan),
    config={"ops": {"source_snapshot": {"config": {"live": True}}, "current_state": {"config": {"live": False}}}},
    description="Acquire fresh public source data and prepare a plan without WarmHub credentials.",
)

validate_sync = dg.define_asset_job(
    "validate_sync",
    selection=dg.AssetSelection.assets(server_validation).upstream()
    | dg.AssetSelection.checks_for_assets(operation_plan),
    config={"ops": {"source_snapshot": {"config": {"live": True}}, "current_state": {"config": {"live": True}}}},
    description="Validate one checked plan with WarmHub without writing it.",
)

synchronize = dg.define_asset_job(
    "synchronize",
    selection=dg.AssetSelection.assets(residual_plan).upstream()
    | dg.AssetSelection.checks_for_assets(operation_plan, residual_plan),
    config={"ops": {"source_snapshot": {"config": {"live": True}}, "current_state": {"config": {"live": True}}}},
    description="Apply one checked plan, read current state again, and require convergence.",
)
