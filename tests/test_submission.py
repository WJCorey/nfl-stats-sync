from __future__ import annotations

import hashlib
import inspect
import json
import uuid
from pathlib import Path

import httpx
import pytest
from warmhub import WarmHubClient
from warmhub._generated.wire import (
    CommitValidateResult,
    CommitValidateResultBaseline1,
    CommitValidateResultCounts,
)
from warmhub.operation_event_identity import operation_event_stream_request_id
from warmhub.errors import WriteOperationError
from warmhub.results import (
    SubmittedOperation, SubmitResult, decode_operation_event_receipt,
)

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
            "shapes": ["Person"],
            "name": "managed/alice",
            "operation": "add",
        }
    )
    operations_path.write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    manifest_path = tmp_path / "plan.json"
    manifest_path.write_bytes(
        canonical_json(
            {
                "format": "mushroom-plan/v2",
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
            "shapes": ["Person"],
            "name": "managed/alice",
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
            "about": "managed/arc@v1",
            "data": {},
            "expectedVersion": 1,
            "shapes": ["Assignment"],
            "name": "managed/assignment",
            "operation": "revise",
        }
    )
    operations_path.write_bytes(payload)
    manifest_path = tmp_path / "plan.json"
    manifest_path.write_bytes(
        canonical_json(
            {
                "format": "mushroom-plan/v2",
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
    assert uuid.UUID(_batch_submission_id("organization/repository", "a" * 64, "c" * 64, 6)).version == 5


def test_batch_submission_ids_are_deterministic_distinct_and_sdk_canonical() -> None:
    first = _batch_submission_id("organization/repository", "a" * 64, "c" * 64, 6)

    assert first == _batch_submission_id("organization/repository", "a" * 64, "c" * 64, 6)
    assert first != _batch_submission_id("organization/repository", "b" * 64, "c" * 64, 6)
    assert first != _batch_submission_id("organization/repository", "a" * 64, "c" * 64, 7)
    assert first != _batch_submission_id("organization/other", "a" * 64, "c" * 64, 6)
    assert first != _batch_submission_id("organization/repository", "a" * 64, "d" * 64, 6)
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
        "shapes": ["Person"],
        "name": "managed/alice",
        "operation": "add",
    }
    rows = [{"status": "applied"}, {"status": "unknown"}]
    response = SubmitResult(
        operation_count=1,
        operations=(
            SubmittedOperation(
                name="managed/alice",
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


def test_v1_manifest_is_rejected_even_with_matching_operation_hash(tmp_path: Path) -> None:
    operations_path, digest, manifest_path = _checked_paths(tmp_path)
    manifest = json.loads(manifest_path.read_bytes())
    del manifest["format"]
    manifest_path.write_bytes(canonical_json(manifest))
    with pytest.raises(SubmissionError, match="v2"):
        _checked_plan(target="ericliu0000/mushroom-integration",
                      operations_path=operations_path, plan_sha256=digest,
                      plan_manifest_path=manifest_path)


@pytest.mark.parametrize("change", [
    {"kind": "thing"}, {"operation": "create"}, {"name": "hub/shapes/Person"},
    {"shapes": "Person"}, {"expectedVersion": 1}, {"name": "managed/alice@v1"},
])
def test_checked_plan_refuses_invalid_v2_operation(tmp_path: Path, change: dict) -> None:
    operations_path, _, manifest_path = _checked_paths(tmp_path)
    operation = json.loads(operations_path.read_bytes())
    operation.update(change)
    payload = canonical_json(operation)
    operations_path.write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    manifest = json.loads(manifest_path.read_bytes())
    manifest["operations"] = digest
    manifest_path.write_bytes(canonical_json(manifest))
    with pytest.raises(SubmissionError):
        _checked_plan(target="ericliu0000/mushroom-integration",
                      operations_path=operations_path, plan_sha256=digest,
                      plan_manifest_path=manifest_path)


def test_v2_noop_receipt_retains_the_submitted_revise_verb() -> None:
    operation = {
        "operation": "revise", "name": "managed/alice", "expectedVersion": 1,
        "shapes": ["Person"], "data": {"label": "Alice"},
    }
    submission_id = "00000000-0000-4000-8000-000000000001"

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("diagnostics.capabilities"):
            payload = {"minSupportedClients": {"warmhub-sdk-py": "0.24.0"},
                       "writeContractVersion": 3}
        else:
            assert request.url.path.endswith("stream.append")
            payload = {
                "event": None, "outcome": "no_event",
                "eventRequestId": operation_event_stream_request_id(submission_id, 0),
                "submissionId": submission_id,
                "requestDigest": "sha256:" + "a" * 64,
                "schemaVersion": "operation-event-receipt/v2",
                "operations": [{
                    "opIndex": 0, "operation": "revise", "name": "managed/alice",
                    "status": "noop", "version": 1, "dataHash": "sha256:" + "b" * 64,
                }],
            }
        envelope = {"result": {"data": payload}}
        return httpx.Response(200, json=[envelope] if request.url.params.get("batch") == "1" else envelope)

    with httpx.Client(transport=httpx.MockTransport(handler)) as http_client:
        with WarmHubClient(api_url="http://warmhub.test", access_token="synthetic-test-token", http_client=http_client) as client:
            result = client.repository("organization/repository").apply(
                "unchanged revise", (operation,), submission_id=submission_id, retry=False,
            )
    rows = [{"status": "unknown"}]
    _record_result_rows(
        result, (operation,), rows, target="organization/repository", plan_sha256="a" * 64,
    )
    assert rows[0]["status"] == "noop"
    assert rows[0]["operation"] == "revise"


def test_sdk_write_operation_error_is_wrapped_at_the_plan_boundary() -> None:
    with pytest.raises(SubmissionError, match="pinned WarmHub SDK") as caught:
        submission._validate_operation({
            "operation": "add", "name": "managed/alice",
            "shapes": ["Person@v1"], "data": {},
        })
    assert isinstance(caught.value.__cause__, WriteOperationError)


def test_fresh_state_manifest_changes_submission_id_for_identical_plan(tmp_path: Path) -> None:
    operations_path, digest, manifest_path = _checked_paths(tmp_path)
    submission_ids = []

    class Repository:
        def validate(self, operations, **kwargs):
            return _validation_response()

        def apply(self, message, operations, **kwargs):
            submission_ids.append(kwargs["submission_id"])
            return SubmitResult(operation_count=1, operations=(SubmittedOperation(
                name="managed/alice", operation="add", data_hash="sha256:example",
                version=1, status="applied",
            ),))

    class Client:
        def repository(self, target):
            return Repository()

    def apply():
        submission.apply_plan(
            Client(), target="ericliu0000/mushroom-integration",
            operations_path=operations_path, plan_sha256=digest,
            plan_manifest_path=manifest_path, results_path=tmp_path / "results.jsonl",
        )

    apply()
    apply()
    assert submission_ids[0] == submission_ids[1]
    manifest = json.loads(manifest_path.read_bytes())
    manifest["current"] = "changed-after-add-and-retraction"
    manifest_path.write_bytes(canonical_json(manifest))
    apply()
    assert submission_ids[2] != submission_ids[0]


_COALESCED_WARNING = {
    "coalescedWrefs": [{"fieldPath": "parent", "wref": "absent", "reason": "thing_absent"}],
    "totalCoalescedWrefs": 1,
}
_IGNORED_PIN_WARNING = {
    "ignoredVersionPins": [{"fieldPath": "parent", "wref": "parent@v1", "binding": "identity"}],
}


def _warning_transport(*, preview_warnings=None, receipt_warnings=None, effect=False):
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.url.path.endswith("commit.validate"):
            row = {"opIndex": 0, "operation": "add", "name": "managed/alice", "status": "would_apply", "version": 1}
            if preview_warnings is not None:
                if effect:
                    row["effects"] = [{"effectIndex": 0, "operation": "add", "name": "managed/alice", "status": "would_apply", "warnings": preview_warnings}]
                else:
                    row["warnings"] = preview_warnings
            payload = {
                "operationCount": 1, "canCommit": True,
                "counts": {"wouldApply": 1, "noop": 0, "error": 0},
                "baseline": {"kind": "withheld"}, "caveats": [], "operations": [row],
            }
        elif request.url.path.endswith("diagnostics.capabilities"):
            payload = {"minSupportedClients": {"warmhub-sdk-py": "0.24.0"}, "writeContractVersion": 3}
        else:
            assert request.url.path.endswith("stream.append")
            body = json.loads(request.content)
            if "0" in body:
                body = body["0"]
            body = body.get("json", body)
            row = {"opIndex": 0, "operation": "add", "name": "managed/alice", "status": "applied", "version": 1, "dataHash": "sha256:" + "b" * 64}
            if receipt_warnings is not None:
                row["warnings"] = receipt_warnings
            payload = {
                "event": {"committedAt": "2026-10-14T00:00:00Z", "eventDigest": "sha256:" + "c" * 64,
                          "operationCount": 1, "repo": {"orgName": "ericliu0000", "repoName": "mushroom-integration"}, "repoSeq": "1"},
                "outcome": "event", "eventRequestId": operation_event_stream_request_id(body["submissionId"], body["chunkOrdinal"]),
                "submissionId": body["submissionId"], "requestDigest": "sha256:" + "a" * 64,
                "schemaVersion": "operation-event-receipt/v2", "operations": [row],
            }
        envelope = {"result": {"data": payload}}
        return httpx.Response(200, json=[envelope] if request.url.params.get("batch") == "1" else envelope)

    return httpx.MockTransport(handler), calls


@pytest.mark.parametrize("warnings", [_COALESCED_WARNING, _IGNORED_PIN_WARNING, {"shapeless": True}])
@pytest.mark.parametrize("effect", [False, True])
def test_sdk_preview_warnings_stop_before_apply_and_preserve_diagnostics(
    tmp_path: Path, warnings: dict, effect: bool,
) -> None:
    operations_path, digest, manifest_path = _checked_paths(tmp_path)
    transport, calls = _warning_transport(preview_warnings=warnings, effect=effect)
    results_path = tmp_path / "results.jsonl"
    with httpx.Client(transport=transport) as http_client:
        with WarmHubClient(api_url="http://warmhub.test", access_token="synthetic-test-token", http_client=http_client) as client:
            with pytest.raises(SubmissionError, match="dry-run"):
                submission.apply_plan(
                    client, target="ericliu0000/mushroom-integration",
                    operations_path=operations_path, plan_sha256=digest,
                    plan_manifest_path=manifest_path, results_path=results_path,
                )
    assert all(path.endswith("commit.validate") for path in calls)
    row = json.loads(results_path.read_bytes())
    assert row["status"] == "not_submitted"
    assert row["version"] is None
    assert row["error"]["warnings"][0]["warnings"] == warnings


@pytest.mark.parametrize("warnings", [_COALESCED_WARNING, _IGNORED_PIN_WARNING, {}])
def test_sdk_receipt_warnings_preserve_actual_applied_evidence(
    tmp_path: Path, warnings: dict,
) -> None:
    operations_path, digest, manifest_path = _checked_paths(tmp_path)
    transport, calls = _warning_transport(receipt_warnings=warnings)
    results_path = tmp_path / "results.jsonl"
    with httpx.Client(transport=transport) as http_client:
        with WarmHubClient(api_url="http://warmhub.test", access_token="synthetic-test-token", http_client=http_client) as client:
            def apply():
                return submission.apply_plan(
                    client, target="ericliu0000/mushroom-integration",
                    operations_path=operations_path, plan_sha256=digest,
                    plan_manifest_path=manifest_path, results_path=results_path,
                )
            if warnings:
                with pytest.raises(SubmissionError, match="applied a batch with warnings"):
                    apply()
            else:
                assert apply().operation_count == 1
    assert sum(path.endswith("stream.append") for path in calls) == 1
    row = json.loads(results_path.read_bytes())
    assert row["status"] == "applied"
    assert row["version"] == 1
    assert row["warnings"] == warnings


def test_warned_apply_stops_later_batches_without_discarding_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    operations_path, _, manifest_path = _checked_paths(tmp_path)
    operation = json.loads(operations_path.read_bytes())
    operations_path.write_bytes(canonical_json(operation) + canonical_json({**operation, "name": "managed/bob"}))
    digest = hashlib.sha256(operations_path.read_bytes()).hexdigest()
    manifest = json.loads(manifest_path.read_bytes())
    manifest["operations"] = digest
    manifest_path.write_bytes(canonical_json(manifest))
    monkeypatch.setattr(submission, "_COMMIT_MAX_OPERATIONS", 1)
    transport, calls = _warning_transport(receipt_warnings=_COALESCED_WARNING)
    results_path = tmp_path / "results.jsonl"
    with httpx.Client(transport=transport) as http_client:
        with WarmHubClient(api_url="http://warmhub.test", access_token="synthetic-test-token", http_client=http_client) as client:
            with pytest.raises(SubmissionError, match="applied a batch with warnings"):
                submission.apply_plan(
                    client, target="ericliu0000/mushroom-integration",
                    operations_path=operations_path, plan_sha256=digest,
                    plan_manifest_path=manifest_path, results_path=results_path,
                )
    rows = [json.loads(line) for line in results_path.read_bytes().splitlines()]
    assert [row["status"] for row in rows] == ["applied", "not_submitted"]
    assert rows[0]["warnings"] == _COALESCED_WARNING
    assert rows[0]["version"] == 1
    assert sum(path.endswith("stream.append") for path in calls) == 1
