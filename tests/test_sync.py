from __future__ import annotations

import hashlib
import inspect
import json
from pathlib import Path

import dagster as dg
import pytest

from mushroom.definitions import defs
from mushroom.defs.sync import _live_target, synchronization_results
from mushroom.kernel import LocalArtifactStore, canonical_jsonl
from project import domain


def test_prepare_sync_remains_fixture_only(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MUSHROOM_HOME", str(tmp_path))

    result = defs().resolve_job_def("prepare_sync").execute_in_process()

    assert result.success
    assert LocalArtifactStore(tmp_path).resolve("operations")[1]
    manifest = json.loads(LocalArtifactStore(tmp_path).resolve("plan")[1])
    assert manifest["format"] == "mushroom-plan/v2"
    assert manifest["shapeFields"] == domain.SHAPE_FIELDS
    assert manifest["policy"]["allowShapeChanges"] is False
    assert manifest["policy"]["requireExistingRecords"] is True


def test_prepare_fresh_sync_acquires_before_planning(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MUSHROOM_HOME", str(tmp_path))
    monkeypatch.setattr("project.acquisition.acquire", lambda: __import__("project.domain", fromlist=["source_fixture"]).source_fixture().read_bytes())

    result = defs().resolve_job_def("prepare_fresh_sync").execute_in_process()

    assert result.success
    assert LocalArtifactStore(tmp_path).resolve("source-origin")[1] == b'{"source":"public-sources"}\n'


def test_validate_sync_requires_an_enabled_client(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MUSHROOM_HOME", str(tmp_path))

    result = defs().resolve_job_def("validate_sync").execute_in_process(
        resources={"warmhub_client": None}, raise_on_error=False
    )

    assert not result.success
    assert result.failure_data_for_node("current_state") is not None


@pytest.mark.parametrize("target", [None, "example/mushroom-starter"])
def test_live_jobs_require_an_explicit_nonstarter_target(monkeypatch, target: str | None) -> None:
    if target is None:
        monkeypatch.delenv("MUSHROOM_TARGET", raising=False)
    else:
        monkeypatch.setenv("MUSHROOM_TARGET", target)

    with pytest.raises(dg.Failure, match="explicit non-starter MUSHROOM_TARGET"):
        _live_target()


def test_synchronize_does_not_reuse_advisory_validation() -> None:
    job = defs().resolve_job_def("synchronize")

    assert "server_validation" not in job.graph.node_names()
    assert [input_def.name for input_def in job.graph.node_dict["synchronization_results"].definition.input_defs] == [
        "operation_plan"
    ]
    assert "validate_plan(" not in inspect.getsource(
        synchronization_results.node_def.compute_fn.decorated_fn
    )


@pytest.mark.parametrize("setting", ["SHAPE_FIELDS", "ALLOW_SHAPE_CHANGES", "REQUIRE_EXISTING_RECORDS"])
def test_missing_upgrade_setting_fails_before_publishing_a_plan(monkeypatch, tmp_path, setting):
    monkeypatch.setenv("MUSHROOM_HOME", str(tmp_path))
    monkeypatch.delattr(domain, setting)
    result = defs().resolve_job_def("prepare_sync").execute_in_process(raise_on_error=False)
    assert not result.success
    assert result.failure_data_for_node("operation_plan") is not None
    assert not (tmp_path / "refs" / "plan.json").exists()


def test_explicit_declaration_migration_setting_reaches_persisted_plan(monkeypatch, tmp_path):
    monkeypatch.setenv("MUSHROOM_HOME", str(tmp_path))
    monkeypatch.setattr(domain, "ALLOW_SHAPE_CHANGES", True)
    rows = [json.loads(line) for line in domain.current_state_fixture().read_bytes().splitlines()]
    for row in rows:
        row["shapes"] = ["LegacyExampleRecord"]
    current_path = tmp_path / "current.jsonl"
    current_path.write_bytes(canonical_jsonl(rows, current=True))
    monkeypatch.setattr(domain, "current_state_fixture", lambda: current_path)
    result = defs().resolve_job_def("prepare_sync").execute_in_process()
    assert result.success
    manifest = json.loads(LocalArtifactStore(tmp_path).resolve("plan")[1])
    assert manifest["policy"]["allowShapeChanges"] is True
    operations = [json.loads(line) for line in LocalArtifactStore(tmp_path).resolve("operations")[1].splitlines()]
    assert any(op["operation"] == "revise" and op["shapes"] != ["LegacyExampleRecord"] for op in operations)


def test_contracts_are_canonicalized_before_planning(monkeypatch, tmp_path):
    monkeypatch.setenv("MUSHROOM_HOME", str(tmp_path))
    fields = {**domain.SHAPE_FIELDS, "Player": {**domain.SHAPE_FIELDS["Player"], "optionalRefs?": ("wref",)}}
    monkeypatch.setattr(domain, "SHAPE_FIELDS", fields)
    from mushroom.defs import sync
    actual_reconcile = sync.reconcile
    observed = []
    def reconcile(*args, **kwargs):
        observed.append(kwargs["reference_fields"])
        return actual_reconcile(*args, **kwargs)
    monkeypatch.setattr(sync, "reconcile", reconcile)
    assert defs().resolve_job_def("prepare_sync").execute_in_process().success
    manifest = json.loads(LocalArtifactStore(tmp_path).resolve("plan")[1])
    assert observed == [manifest["shapeFields"]]
    assert manifest["shapeFields"]["Player"]["optionalRefs?"] == ["wref"]


@pytest.mark.parametrize("changed", ["desired", "source"])
def test_readback_rejects_inputs_from_another_plan(monkeypatch, tmp_path, changed):
    from mushroom.defs.sync import residual_plan
    from mushroom.kernel import KernelError
    monkeypatch.setenv("MUSHROOM_HOME", str(tmp_path))
    assert defs().resolve_job_def("prepare_sync").execute_in_process().success
    store = LocalArtifactStore(tmp_path)
    store.publish("readback", store.reference("current"))
    payload = b'{"complete":true}\n' if changed == "source" else b""
    store.publish(changed, store.persist(payload))
    with pytest.raises(KernelError, match="inputs differ"):
        residual_plan()


def test_existing_record_expectation_reaches_manifest(monkeypatch, tmp_path):
    monkeypatch.setenv("MUSHROOM_HOME", str(tmp_path))
    monkeypatch.setattr(domain, "REQUIRE_EXISTING_RECORDS", True)
    assert defs().resolve_job_def("prepare_sync").execute_in_process().success
    manifest = json.loads(LocalArtifactStore(tmp_path).resolve("plan")[1])
    assert manifest["policy"]["requireExistingRecords"] is True


def test_complete_retraction_can_converge_after_guarded_baseline(monkeypatch, tmp_path):
    from mushroom.defs.sync import residual_plan
    monkeypatch.setenv("MUSHROOM_HOME", str(tmp_path))
    monkeypatch.setattr(domain, "REQUIRE_EXISTING_RECORDS", True)
    monkeypatch.setattr(domain, "ABSENCE_POLICY", "retract")
    source = tmp_path / "source.json"
    source.write_text('{"complete":true}')
    monkeypatch.setattr(domain, "source_fixture", lambda: source)
    monkeypatch.setattr(domain, "transform", lambda _source: [])
    assert defs().resolve_job_def("prepare_sync").execute_in_process().success
    store = LocalArtifactStore(tmp_path)
    rows = [json.loads(line) for line in store.resolve("current")[1].splitlines()]
    readback = [dict(row, active=False, version=row["version"] + 1) for row in rows]
    store.publish("readback", store.persist(canonical_jsonl(readback, current=True)))
    residual_plan()
    assert store.resolve("residual")[1] == b""
