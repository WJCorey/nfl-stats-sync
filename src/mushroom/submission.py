"""Apply one checked operations.jsonl file without replanning it."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Protocol

from warmhub.errors import AllStreamOperationsFailedError
from warmhub.operations import to_backend_stream_operation

from mushroom.kernel import KernelError, canonical_json


_TARGET = re.compile(r"^[a-z0-9][a-z0-9.-]*/[a-z0-9][a-z0-9.-]*$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
_COMMIT_MAX_OPERATIONS = 1_000
_COMMIT_MAX_BYTES = 3_500_000
_VALIDATE_OPERATION_LIMIT = 10_000
_VALIDATE_MAX_BYTES = 4 * 1024 * 1024


class SubmissionError(KernelError):
    """A plan was not accepted for validation or completely applied."""


class _Repository(Protocol):
    def validate(
        self,
        operations: Sequence[Mapping[str, object]],
        *,
        message: str,
        include_would_be_body: bool = False,
    ) -> object: ...

    def apply(
        self,
        message: str,
        operations: Sequence[Mapping[str, object]],
        *,
        chunk_size: int,
        submission_id: str,
        retry: bool = False,
    ) -> object: ...


class _Client(Protocol):
    def repository(self, target: str) -> _Repository: ...


@dataclass(frozen=True, slots=True)
class PlanApplication:
    plan_sha256: str
    operation_count: int
    results_sha256: str


@dataclass(frozen=True, slots=True)
class PlanValidation:
    operation_count: int
    would_apply_count: int
    noop_count: int


def iter_operation_batches(
    operations: Sequence[dict[str, object]],
) -> tuple[tuple[dict[str, object], ...], ...]:
    """Preserve the checked operation order in bounded server requests."""

    batches: list[tuple[dict[str, object], ...]] = []
    batch: list[dict[str, object]] = []
    byte_length = 0
    for operation in operations:
        operation_bytes = len(canonical_json(operation))
        if operation_bytes > _COMMIT_MAX_BYTES:
            raise SubmissionError("one operation exceeds the safe WarmHub commit payload size")
        if batch and (
            len(batch) == _COMMIT_MAX_OPERATIONS
            or byte_length + operation_bytes > _COMMIT_MAX_BYTES
        ):
            batches.append(tuple(batch))
            batch, byte_length = [], 0
        batch.append(operation)
        byte_length += operation_bytes
    if batch:
        batches.append(tuple(batch))
    return tuple(batches)


def validate_plan(
    client: _Client,
    *,
    target: str,
    operations_path: Path,
    plan_sha256: str,
    plan_manifest_path: Path,
) -> PlanValidation:
    """Ask WarmHub to validate one checked plan without submitting it."""

    operations = _checked_plan(
        target=target,
        operations_path=operations_path,
        plan_sha256=plan_sha256,
        plan_manifest_path=plan_manifest_path,
    )
    if not operations:
        return PlanValidation(0, 0, 0)
    if (
        len(operations) > _VALIDATE_OPERATION_LIMIT
        or len(canonical_json(list(operations))) > _VALIDATE_MAX_BYTES
    ):
        raise SubmissionError(
            "standalone server validation unavailable for this plan size; synchronize performs authoritative validate-then-apply per batch"
        )
    try:
        response = client.repository(target).validate(
            operations,
            message=f"Mushroom plan {plan_sha256[:12]}",
            include_would_be_body=False,
        )
    except Exception as error:  # noqa: BLE001 - retain the SDK's safe error boundary.
        raise SubmissionError("WarmHub could not validate the plan") from error
    return _validation_summary(response, operation_count=len(operations))


def _validation_summary(
    response: object, *, operation_count: int
) -> PlanValidation:
    count = _whole_count(getattr(response, "operation_count", None))
    can_commit = getattr(response, "can_commit", None)
    counts = getattr(response, "counts", None)
    would_apply = _whole_count(getattr(counts, "would_apply", None))
    noop = _whole_count(getattr(counts, "noop", None))
    errors = _whole_count(getattr(counts, "error", None))
    if (
        count != operation_count
        or type(can_commit) is not bool
        or would_apply is None
        or noop is None
        or errors is None
        or errors != 0
        or would_apply + noop + errors != operation_count
    ):
        raise SubmissionError("WarmHub dry-run result does not match the submitted plan")
    if not can_commit:
        raise SubmissionError("WarmHub rejected the plan during dry-run")
    return PlanValidation(
        operation_count=operation_count,
        would_apply_count=would_apply,
        noop_count=noop,
    )


def _whole_count(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        return None
    integer = int(value)
    return integer if integer >= 0 and integer == value else None


def apply_plan(
    client: _Client,
    *,
    target: str,
    operations_path: Path,
    plan_sha256: str,
    plan_manifest_path: Path,
    results_path: Path,
) -> PlanApplication:
    """Submit the bytes identified by ``plan_sha256`` once, then save outcomes.

    A failure is terminal for this plan. A caller must capture current state and
    create a fresh plan before trying again.
    """

    if not isinstance(results_path, Path):
        raise SubmissionError("submission requires a results path")
    operations = _checked_plan(
        target=target,
        operations_path=operations_path,
        plan_sha256=plan_sha256,
        plan_manifest_path=plan_manifest_path,
    )
    try:
        if results_path.resolve() in {
            operations_path.resolve(),
            plan_manifest_path.resolve(),
        }:
            raise SubmissionError("results path must not replace a plan input")
    except OSError as error:
        raise SubmissionError("results path cannot be resolved") from error
    rows = [
        _not_submitted_row(ordinal, operation, target=target, plan_sha256=plan_sha256)
        for ordinal, operation in enumerate(operations)
    ]
    if not operations:
        results_sha256 = _write_results(results_path, rows)
        return PlanApplication(plan_sha256, 0, results_sha256)

    repository = client.repository(target)
    batches = iter_operation_batches(operations)
    start = 0
    for batch_index, batch in enumerate(batches):
        stop = start + len(batch)
        message = f"Mushroom plan {plan_sha256[:12]} batch {batch_index + 1}/{len(batches)}"
        try:
            validated = repository.validate(
                batch, message=message, include_would_be_body=False
            )
        except Exception as error:  # noqa: BLE001 - no request to apply this batch occurred.
            _mark_not_submitted(rows[start:], error)
            _write_results(results_path, rows)
            raise SubmissionError(
                "WarmHub could not validate a batch; capture fresh state and replan before another write"
            ) from None
        try:
            _validation_summary(validated, operation_count=len(batch))
        except SubmissionError as error:
            _mark_not_submitted(rows[start:], error)
            _write_results(results_path, rows)
            raise SubmissionError(
                "WarmHub rejected a batch during dry-run; capture fresh state and replan before another write"
            ) from None
        _mark_unknown(rows[start:stop], RuntimeError("submission pending"))
        try:
            response = repository.apply(
                message,
                batch,
                chunk_size=len(batch),
                submission_id=_batch_submission_id(target, plan_sha256, batch_index),
                retry=False,
            )
        except AllStreamOperationsFailedError as error:
            try:
                _record_operations(
                    error.operations,
                    batch,
                    rows,
                    target=target,
                    plan_sha256=plan_sha256, start_ordinal=start,
                )
            except SubmissionError:
                _mark_unknown(rows[start:stop], error)
            _write_results(results_path, rows)
            raise SubmissionError(
                "WarmHub rejected a batch; inspect persisted per-operation results before replanning"
            ) from None
        except Exception as error:  # noqa: BLE001 - an interrupted request has unknown outcome.
            _mark_unknown(rows[start:stop], error)
            _write_results(results_path, rows)
            raise SubmissionError(
                "WarmHub submission is uncertain; capture fresh state and replan before another write"
            ) from None
        try:
            _record_result_rows(
                response,
                batch,
                rows,
                target=target,
                plan_sha256=plan_sha256,
                start_ordinal=start,
            )
        except SubmissionError as error:
            _write_results(results_path, rows)
            raise SubmissionError(
                f"{error}; capture fresh state and replan before another write"
            ) from None
        statuses = [row["status"] for row in rows[start:stop]]
        if bool(getattr(response, "partial", False)) or any(
            status not in {"applied", "noop"} for status in statuses
        ):
            _write_results(results_path, rows)
            raise SubmissionError(
                "WarmHub did not completely apply a batch; capture fresh state and replan before another write"
            )
        start = stop
    results_sha256 = _write_results(results_path, rows)
    return PlanApplication(
        plan_sha256=plan_sha256,
        operation_count=len(operations),
        results_sha256=results_sha256,
    )


def _checked_plan(
    *,
    target: str,
    operations_path: Path,
    plan_sha256: str,
    plan_manifest_path: Path,
) -> tuple[dict[str, object], ...]:
    if not isinstance(target, str) or _TARGET.fullmatch(target) is None:
        raise SubmissionError("submission target must be an unpinned organization/repository")
    if not isinstance(plan_sha256, str) or _SHA256.fullmatch(plan_sha256) is None:
        raise SubmissionError("operation plan identity must be a SHA-256 digest")
    if not all(isinstance(path, Path) for path in (operations_path, plan_manifest_path)):
        raise SubmissionError("submission requires plan and manifest paths")
    try:
        payload = operations_path.read_bytes()
    except OSError as error:
        raise SubmissionError("operation plan cannot be read") from error
    if hashlib.sha256(payload).hexdigest() != plan_sha256:
        raise SubmissionError("operation plan bytes differ from their checked identity")
    _validate_plan_manifest(plan_manifest_path, target=target, plan_sha256=plan_sha256)
    return _parse_plan(payload)


def _validate_plan_manifest(path: Path, *, target: str, plan_sha256: str) -> None:
    try:
        payload = path.read_bytes()
        manifest = json.loads(payload)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SubmissionError("operation plan manifest cannot be read") from error
    if (
        not isinstance(manifest, dict)
        or canonical_json(manifest) != payload
        or manifest.get("operations") != plan_sha256
        or not isinstance(manifest.get("policy"), dict)
        or manifest["policy"].get("target") != target
    ):
        raise SubmissionError("operation plan manifest does not bind this plan to the submission target")


def _parse_plan(payload: bytes) -> tuple[dict[str, object], ...]:
    try:
        rows = [json.loads(line) for line in payload.splitlines() if line.strip()]
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SubmissionError("operation plan is not valid JSONL") from error
    operations: list[dict[str, object]] = []
    identities: set[tuple[object, ...]] = set()
    for row in rows:
        if not isinstance(row, dict):
            raise SubmissionError("operation plan rows must be objects")
        operation = dict(row)
        _validate_operation(operation)
        identity = (
            operation["kind"],
            operation["name"],
            operation.get("type") if operation["kind"] == "collection" and operation["operation"] == "add" else None,
        )
        if identity in identities:
            raise SubmissionError("operation plan contains duplicate names")
        identities.add(identity)
        operations.append(operation)
    return tuple(operations)


def _validate_operation(operation: dict[str, object]) -> None:
    verb, kind, name = operation.get("operation"), operation.get("kind"), operation.get("name")
    if verb not in {"add", "revise", "retract"} or kind not in {"thing", "collection", "assertion"}:
        raise SubmissionError("operation plan contains an unsupported operation")
    if not isinstance(name, str) or not name or "@" in name:
        raise SubmissionError("operation plan names must be unpinned")
    if not (kind == "collection" and verb == "add") and "/" not in name:
        raise SubmissionError("operation plan names must be unpinned wrefs")
    expected = operation.get("expectedVersion")
    if verb == "add":
        if "expectedVersion" in operation:
            raise SubmissionError("add operations cannot have expected versions")
    elif isinstance(expected, bool) or not isinstance(expected, int) or expected < 1:
        raise SubmissionError("revisions and retractions need a positive expected version")
    if verb == "retract":
        if set(operation) != {"operation", "kind", "name", "expectedVersion"}:
            raise SubmissionError("retraction operations contain unsupported fields")
    else:
        fields = {"operation", "kind", "name"}
        if verb == "revise":
            fields.add("expectedVersion")
        if kind == "collection":
            fields.update({"type", "members"})
            valid = isinstance(operation.get("type"), str) and isinstance(operation.get("members"), list)
        elif kind == "assertion":
            fields.add("data")
            if verb == "add":
                fields.add("about")
            valid = isinstance(operation.get("data"), dict) and (
                verb != "add" or isinstance(operation.get("about"), str)
            )
        else:
            fields.add("data")
            valid = isinstance(operation.get("data"), dict)
        if set(operation) != fields or not valid:
            raise SubmissionError("operation plan fields do not match its operation kind")
    try:
        normalized = to_backend_stream_operation(operation)  # type: ignore[arg-type]
    except (TypeError, ValueError) as error:
        raise SubmissionError("operation is rejected by the pinned WarmHub SDK") from error
    if normalized != operation:
        raise SubmissionError("operation differs from the pinned WarmHub SDK wire contract")


def _record_result_rows(
    response: object,
    operations: Sequence[dict[str, object]],
    rows: list[dict[str, object]],
    *,
    target: str,
    plan_sha256: str,
    start_ordinal: int = 0,
) -> None:
    count, submitted = getattr(response, "operation_count", None), getattr(response, "operations", None)
    if isinstance(count, bool) or not isinstance(count, int) or count != len(operations) or not isinstance(submitted, Sequence) or len(submitted) != len(operations):
        raise SubmissionError("WarmHub result count differs from the submitted plan")
    _record_operations(
        submitted,
        operations,
        rows,
        target=target,
        plan_sha256=plan_sha256,
        start_ordinal=start_ordinal,
    )


def _record_operations(
    results: Sequence[object],
    operations: Sequence[dict[str, object]],
    rows: list[dict[str, object]],
    *,
    target: str,
    plan_sha256: str,
    start_ordinal: int = 0,
) -> None:
    if len(results) != len(operations) or len(rows) < start_ordinal + len(operations):
        raise SubmissionError("WarmHub result count differs from the submitted plan")
    for ordinal, (operation, result) in enumerate(zip(operations, results, strict=True), start=start_ordinal):
        rows[ordinal] = _observed_row(
            ordinal, operation, result, target=target, plan_sha256=plan_sha256
        )


def _observed_row(
    ordinal: int,
    operation: dict[str, object],
    result: object,
    *,
    target: str,
    plan_sha256: str,
) -> dict[str, object]:
    submitted_name = getattr(result, "submitted_name", None)
    expected_name = operation["name"]
    if submitted_name is None and operation["kind"] == "collection" and operation["operation"] == "add":
        expected_name = f"{operation['type'].title()}/{expected_name}"
    observed_name = submitted_name or getattr(result, "name", None)
    observed_operation, status = getattr(result, "operation", None), getattr(result, "status", None)
    if observed_name != expected_name or observed_operation != operation["operation"] or status not in {"applied", "noop", "error"}:
        raise SubmissionError("WarmHub result does not match the submitted plan")
    version = getattr(result, "version", None)
    if isinstance(version, bool) or not isinstance(version, int) or version < 0:
        raise SubmissionError("WarmHub result has an invalid version")
    error = getattr(result, "error", None)
    return {
        "dataHash": _string_or_none(getattr(result, "data_hash", None)),
        "error": None if error is None else {"code": _string_or_default(getattr(error, "code", None)), "message": _string_or_default(getattr(error, "message", None))},
        "name": operation["name"],
        "operation": operation["operation"],
        "ordinal": ordinal,
        "planSha256": plan_sha256,
        "resolvedName": _string_or_none(getattr(result, "resolved_name", None)),
        "status": status,
        "target": target,
        "version": version,
    }


def _string_or_none(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _string_or_default(value: object) -> str:
    return value if isinstance(value, str) else "unknown"


def _unknown_row(
    ordinal: int, operation: dict[str, object], *, target: str, plan_sha256: str
) -> dict[str, object]:
    return {
        "dataHash": None,
        "error": None,
        "name": operation["name"],
        "operation": operation["operation"],
        "ordinal": ordinal,
        "planSha256": plan_sha256,
        "resolvedName": None,
        "status": "unknown",
        "target": target,
        "version": None,
    }


def _not_submitted_row(
    ordinal: int, operation: dict[str, object], *, target: str, plan_sha256: str
) -> dict[str, object]:
    row = _unknown_row(ordinal, operation, target=target, plan_sha256=plan_sha256)
    row["status"] = "not_submitted"
    return row


def _mark_unknown(rows: Sequence[dict[str, object]], error: BaseException) -> None:
    details: dict[str, str] = {"code": _safe_error_value(getattr(error, "code", None), type(error).__name__)}
    for attribute, key in (("submission_id", "submissionId"), ("event_request_id", "eventRequestId")):
        value = _safe_error_value(getattr(error, attribute, None), None)
        if value is not None:
            details[key] = value
    for row in rows:
        row.update(dataHash=None, error=details, resolvedName=None, status="unknown", version=None)


def _mark_not_submitted(rows: Sequence[dict[str, object]], error: BaseException) -> None:
    details: dict[str, str] = {"code": _safe_error_value(getattr(error, "code", None), type(error).__name__)}
    for row in rows:
        row.update(dataHash=None, error=details, resolvedName=None, status="not_submitted", version=None)


def _batch_submission_id(target: str, plan_sha256: str, batch_index: int) -> str:
    return str(
        uuid.uuid5(
            uuid.NAMESPACE_URL, f"mushroom:{target}:{plan_sha256}:{batch_index}"
        )
    )


def _safe_error_value(value: object, default: str | None) -> str | None:
    return value if isinstance(value, str) and _SAFE_IDENTIFIER.fullmatch(value) else default


def _write_results(path: Path, rows: Sequence[dict[str, object]]) -> str:
    payload = b"".join(canonical_json(row) for row in rows)
    digest = hashlib.sha256(payload).hexdigest()
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=path.parent, delete=False) as temporary:
        temporary_path = Path(temporary.name)
        try:
            temporary.write(payload)
            temporary.flush()
            os.fsync(temporary.fileno())
            temporary_path.replace(path)
        finally:
            temporary_path.unlink(missing_ok=True)
    return digest
