from __future__ import annotations

from copy import deepcopy
import hashlib
from pathlib import Path
from types import SimpleNamespace

from warmhub._generated.wire import (
    CommitValidateResult, CommitValidateResultBaseline1, CommitValidateResultCounts,
)
from warmhub.results import SubmitResult, SubmittedOperation

from mushroom.current_state import capture_current_state
from mushroom.kernel import ScopePolicy, canonical_json, canonical_jsonl, parse_jsonl, reconcile
from mushroom.submission import apply_plan


def test_add_retract_readd_keeps_identical_plan_but_uses_fresh_submission_identity(tmp_path: Path) -> None:
    class Repository:
        def __init__(self):
            self.rows = []
            self.receipts = {}
            self.submission_ids = []
            self.writes = 0
            self.things = SimpleNamespace(head_iter=self.head_iter, get_many=self.get_many)

        def head_iter(self, **options):
            return [SimpleNamespace(kind="thing", as_dict=lambda row=row: deepcopy(row))
                    for row in self.rows if row["active"] or options["include_retracted"]]

        def get_many(self, references, **options):
            assert references == ["hub/shapes/Person@v1"]
            return SimpleNamespace(missing=[], items=[SimpleNamespace(
                kind="shape", data={"fields": {"label": "string"}},
            )])

        def validate(self, operations, **options):
            return CommitValidateResult(
                operation_count=1, can_commit=True,
                counts=CommitValidateResultCounts(would_apply=1, noop=0, error=0),
                baseline=CommitValidateResultBaseline1(kind="withheld"),
                caveats=(), operations=(),
            )

        def apply(self, message, operations, **options):
            submission_id = options["submission_id"]
            self.submission_ids.append(submission_id)
            if submission_id in self.receipts:
                return self.receipts[submission_id]
            self.writes += 1
            operation, = operations
            assert operation["operation"] == "add"
            self.rows.append({
                "name": operation["name"], "kind": "thing", "active": True,
                "version": 1, "declaredShapes": ["Person@v1"],
                "validatedShapes": ["Person@v1"], "data": operation["data"],
                "metadata": {"durableId": f"identity-{self.writes}"},
            })
            result = SubmitResult(operation_count=1, operations=(SubmittedOperation(
                name=operation["name"], operation="add", data_hash="sha256:example",
                version=1, status="applied",
            ),))
            self.receipts[submission_id] = result
            return result

        def retract(self):
            self.rows[-1].update(active=False, version=2)

    repository = Repository()
    client = SimpleNamespace(repository=lambda _: repository)
    desired_path, current_path = tmp_path / "desired.jsonl", tmp_path / "current.jsonl"
    operations_path, manifest_path = tmp_path / "operations.jsonl", tmp_path / "plan.json"
    desired_path.write_bytes(canonical_jsonl([
        {"name": "managed/alice", "shapes": ["Person"], "data": {"label": "Alice"}},
    ]))

    def capture_plan_and_apply():
        _, current = capture_current_state(client, target="example/repo", pattern="managed/**")
        current_path.write_bytes(current)
        plan = reconcile(desired_path, current_path, operations_path,
                         ScopePolicy("example/repo", "managed/**", True, "preserve"),
                         reference_fields={"Person": {}})
        manifest_path.write_bytes(canonical_json({
            "format": "mushroom-plan/v2", "operations": plan.sha256,
            "current": hashlib.sha256(current).hexdigest(),
            "desired": hashlib.sha256(desired_path.read_bytes()).hexdigest(),
            "policy": {"target": "example/repo"},
        }))
        operations = operations_path.read_bytes()
        manifest = manifest_path.read_bytes()
        apply_plan(client, target="example/repo", operations_path=operations_path,
                   plan_sha256=plan.sha256, plan_manifest_path=manifest_path,
                   results_path=tmp_path / "results.jsonl")
        return current, operations, manifest

    initial_current, first_operations, first_manifest = capture_plan_and_apply()
    repository.retract()
    retracted_current, second_operations, second_manifest = capture_plan_and_apply()

    assert first_operations == second_operations
    assert initial_current != retracted_current
    assert first_manifest != second_manifest
    tombstone, = parse_jsonl(retracted_current)
    assert tombstone["active"] is False
    assert tombstone["version"] == 2
    assert tombstone["shapes"] == ["Person"]
    assert tombstone["validatedShapes"] == ["Person@v1"]
    assert repository.submission_ids[0] != repository.submission_ids[1]
    assert repository.writes == 2
    _, reactivated_current = capture_current_state(client, target="example/repo", pattern="managed/**")
    reactivated, = parse_jsonl(reactivated_current)
    assert reactivated["active"] is True
    assert reactivated["durableId"] == "identity-2"
