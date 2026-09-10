from __future__ import annotations

import json
from pathlib import Path

import pytest

from mushroom.kernel import ScopePolicy, canonical_json, canonical_jsonl, reconcile
from project import domain


def test_target_uses_environment_with_a_harmless_default(monkeypatch) -> None:
    monkeypatch.delenv("MUSHROOM_TARGET", raising=False)
    assert domain.target() == "example/mushroom-starter"
    monkeypatch.setenv("MUSHROOM_TARGET", "organization/repository")
    assert domain.target() == "organization/repository"


def test_synthetic_transform_matches_the_frozen_desired_state() -> None:
    desired = canonical_jsonl(domain.transform(domain.source_fixture().read_bytes()))

    assert desired == (domain.source_fixture().parent / "desired.jsonl").read_bytes()


@pytest.mark.parametrize("identifier", [None, "", "UPPER", "contains/slash", "-leading"])
def test_invalid_synthetic_identifiers_fail_visibly(identifier: object) -> None:
    source = json.loads(domain.source_fixture().read_bytes())
    source["records"][0]["id"] = identifier

    with pytest.raises(ValueError, match="invalid synthetic record id"):
        domain.transform(canonical_json(source))


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"complete": True, "records": [{"id": "alpha", "label": "Alpha"}]},
        {"complete": False, "records": []},
        {"complete": False, "records": [None]},
        {"complete": False, "records": [{"id": "alpha", "label": " "}]},
        {
            "complete": False,
            "records": [{"id": "alpha", "label": "A"}, {"id": "alpha", "label": "B"}],
        },
    ],
)
def test_malformed_synthetic_snapshots_fail_visibly(payload: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        domain.transform(canonical_json(payload))


def test_incomplete_example_preserves_missing_current_records(tmp_path: Path) -> None:
    desired_path, operations_path = tmp_path / "desired.jsonl", tmp_path / "operations.jsonl"
    desired_path.write_bytes(canonical_jsonl(domain.transform(domain.source_fixture().read_bytes())))

    result = reconcile(
        desired_path,
        domain.current_state_fixture(),
        operations_path,
        ScopePolicy(domain.target(), domain.MANAGED_SCOPE, False, domain.ABSENCE_POLICY),
    )

    assert operations_path.read_bytes() == (domain.source_fixture().parent / "operations.jsonl").read_bytes()
    assert (
        result.summary.add_count,
        result.summary.revise_count,
        result.summary.retract_count,
        result.summary.unchanged_count,
        result.summary.preserved_count,
    ) == (1, 1, 0, 0, 1)
