from __future__ import annotations

import hashlib
import inspect
import uuid
from pathlib import Path

import pytest
from warmhub._generated.wire import (
    CommitValidateResult,
    CommitValidateResultBaseline1,
    CommitValidateResultCounts,
)
from warmhub.operation_event_identity import operation_event_stream_request_id
from warmhub.results import SubmittedOperation, SubmitResult, decode_operation_event_receipt

from mushroom.kernel import canonical_json
from mushroom import submission
from mushroom.submission import (
    SubmissionError,
    _batch_submission_id,
    _checked_plan,
    _record_result_rows,
    _validation_summary,
    iter_operation_batches,
)


def _checked_paths(tmp_path: Path) -> tuple[Path, str, Path]:
    operations_path = tmp_path / "operations.jsonl"
    payload = canonical_json(
        {
            "data": {"label": "Alice"},
            "kind": "thing",
            "name": "Person/managed/alice",
            "operation": "add",
        }
    )
    operations_path.write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    manifest_path = tmp_path / "plan.json"
    manifest_path.write_bytes(
        canonical_json(
            {
                "operations": digest,
                "policy": {"target": "ericliu0000/mushroom-integration"},
            }
        )
    )
    return operations_path, digest, manifest_path


def test_checked_plan_reads_exact_persisted_operations(tmp_path: Path) -> None:
    operations_path, digest, manifest_path = _checked_paths(tmp_path)

    assert _checked_plan(
        target="ericliu0000/mushroom-integration",
        operations_path=operations_path,
        plan_sha256=digest,
        plan_manifest_path=manifest_path,
    ) == (
        {
            "data": {"label": "Alice"},
            "kind": "thing",
            "name": "Person/managed/alice",
            "operation": "add",
        },
    )


def test_checked_plan_rejects_changed_bytes_before_any_server_call(tmp_path: Path) -> None:
    operations_path, digest, manifest_path = _checked_paths(tmp_path)
    operations_path.write_bytes(b"{}\n")

    with pytest.raises(SubmissionError, match="checked identity"):
        _checked_plan(
            target="ericliu0000/mushroom-integration",
            operations_path=operations_path,
            plan_sha256=digest,
            plan_manifest_path=manifest_path,
        )


def test_checked_plan_rejects_an_assertion_revision_with_about(tmp_path: Path) -> None:
    operations_path = tmp_path / "operations.jsonl"
    payload = canonical_json(
        {
            "about": "Arc/managed/example@v1",
            "data": {},
            "expectedVersion": 1,
            "kind": "assertion",
            "name": "Assignment/managed/example",
            "operation": "revise",
        }
    )
    operations_path.write_bytes(payload)
    manifest_path = tmp_path / "plan.json"
    manifest_path.write_bytes(
        canonical_json(
            {
                "operations": hashlib.sha256(payload).hexdigest(),
                "policy": {"target": "ericliu0000/mushroom-integration"},
            }
        )
    )

    with pytest.raises(SubmissionError, match="fields do not match"):
        _checked_plan(
            target="ericliu0000/mushroom-integration",
            operations_path=operations_path,
            plan_sha256=hashlib.sha256(payload).hexdigest(),
            plan_manifest_path=manifest_path,
        )


def _validation_response(
    *,
    can_commit: bool = True,
    would_apply: float = 1,
    noop: float = 0,
    error: float = 0,
) -> CommitValidateResult:
    return CommitValidateResult(
        operation_count=1,
        can_commit=can_commit,
        counts=CommitValidateResultCounts(
            would_apply=would_apply, noop=noop, error=error
        ),
        baseline=CommitValidateResultBaseline1(kind="withheld"),
        caveats=(),
        operations=(),
    )


def test_validation_summary_accepts_the_official_sdk_result() -> None:
    summary = _validation_summary(
        _validation_response(), operation_count=1
    )

    assert summary.would_apply_count == 1


def test_official_sdk_decodes_operation_event_receipt_v2() -> None:
    receipt = decode_operation_event_receipt(
        {
            "event": None,
            "eventRequestId": "receipt-v2-test",
            "operations": [],
            "outcome": "no_event",
            "requestDigest": f"sha256:{'0' * 64}",
            "schemaVersion": "operation-event-receipt/v2",
        }
    )

    assert receipt.schema_version == "operation-event-receipt/v2"


def test_large_operation_batches_preserve_order() -> None:
    operations = [{"ordinal": ordinal} for ordinal in range(12_082)]
    batches = iter_operation_batches(operations)

    assert [len(batch) for batch in batches] == [1_000] * 12 + [82]
    assert [row["ordinal"] for batch in batches for row in batch] == list(range(12_082))
    assert uuid.UUID(_batch_submission_id("organization/repository", "a" * 64, 6)).version == 5


def test_batch_submission_ids_are_deterministic_distinct_and_sdk_canonical() -> None:
    first = _batch_submission_id("organization/repository", "a" * 64, 6)

    assert first == _batch_submission_id("organization/repository", "a" * 64, 6)
    assert first != _batch_submission_id("organization/repository", "b" * 64, 6)
    assert first != _batch_submission_id("organization/repository", "a" * 64, 7)
    assert first != _batch_submission_id("organization/other", "a" * 64, 6)
    assert operation_event_stream_request_id(first, 0)


def test_operation_batches_stop_before_the_safe_payload_limit() -> None:
    batches = iter_operation_batches([
        {"payload": "a" * 2_000_000},
        {"payload": "b" * 2_000_000},
    ])

    assert [len(batch) for batch in batches] == [1, 1]


def test_each_checked_batch_is_one_sdk_stream_chunk() -> None:
    assert "chunk_size=len(batch)" in inspect.getsource(submission.apply_plan)


def test_recorded_batch_results_replace_the_original_plan_rows() -> None:
    operation = {
        "data": {"label": "Alice"},
        "kind": "thing",
        "name": "Person/managed/alice",
        "operation": "add",
    }
    rows = [{"status": "applied"}, {"status": "unknown"}]
    response = SubmitResult(
        operation_count=1,
        operations=(
            SubmittedOperation(
                name="Person/managed/alice",
                operation="add",
                data_hash="sha256:example",
                version=1,
                status="applied",
            ),
        ),
    )

    _record_result_rows(
        response,
        (operation,),
        rows,
        target="ericliu0000/mushroom-integration",
        plan_sha256="a" * 64,
        start_ordinal=1,
    )

    assert rows[0] == {"status": "applied"}
    assert rows[1]["status"] == "applied"
    assert rows[1]["ordinal"] == 1


@pytest.mark.parametrize(
    "response",
    [
        _validation_response(can_commit=False),
        _validation_response(would_apply=0.5, noop=0.5),
        _validation_response(error=1),
    ],
)
def test_validation_summary_rejects_noncommittable_or_invalid_results(
    response: CommitValidateResult,
) -> None:
    with pytest.raises(SubmissionError):
        _validation_summary(response, operation_count=1)
