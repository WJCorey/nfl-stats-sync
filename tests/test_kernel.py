from __future__ import annotations

import json
from tempfile import TemporaryDirectory
from pathlib import Path

import pytest
from warmhub.operations import to_backend_stream_operation

from mushroom.kernel import (
    KernelError,
    LocalArtifactStore,
    ScopePolicy,
    canonical_jsonl,
    matches_scope,
    parse_jsonl,
    reconcile as reconcile_paths,
)


def _thing(name: str, label: str, *, version: int | None = None) -> dict[str, object]:
    row: dict[str, object] = {
        "kind": "thing",
        "shape": "Person",
        "wref": f"Person/managed/{name}",
        "data": {"label": label},
    }
    if version is not None:
        row.update(active=True, version=version)
    return row


def reconcile(desired: list[dict[str, object]], current: list[dict[str, object]], policy: ScopePolicy) -> tuple[bytes, object]:
    with TemporaryDirectory() as directory:
        root = Path(directory)
        desired_path, current_path, operations_path = root / "desired.jsonl", root / "current.jsonl", root / "operations.jsonl"
        desired_path.write_bytes(b"".join(canonical_jsonl([row]) for row in desired))
        current_path.write_bytes(b"".join(canonical_jsonl([row], current=True) for row in current))
        result = reconcile_paths(desired_path, current_path, operations_path, policy)
        return operations_path.read_bytes(), result.summary


def test_managed_scope_matching_is_path_aware() -> None:
    assert matches_scope("Person/example/alice", "*/example/**")
    assert not matches_scope("Person/other/example/alice", "*/example/**")
    assert not matches_scope("Person/example", "*/example/**")
    with pytest.raises(KernelError, match="only literal segments"):
        matches_scope("Person/example/alice", "*/{example,demo}/**")
    with pytest.raises(KernelError, match="only literal segments"):
        matches_scope("Person/example/alice", "*/exam*/**")


def test_reconcile_is_deterministic_and_complete() -> None:
    desired = [_thing("add", "new"), _thing("revise", "new label")]
    current = [
        _thing("retract", "gone", version=4),
        _thing("revise", "old label", version=2),
        {**_thing("outside", "keep", version=1), "wref": "Person/outside/keep"},
    ]
    policy = ScopePolicy("warmhub-data/example", "*/managed/**", True, "retract")

    first, summary = reconcile(desired, current, policy)
    second, _ = reconcile(list(reversed(desired)), list(reversed(current)), policy)

    assert first == second
    assert parse_jsonl(first) == [
        {"data": {"label": "new"}, "kind": "thing", "name": "Person/managed/add", "operation": "add"},
        {
            "data": {"label": "new label"},
            "expectedVersion": 2,
            "kind": "thing",
            "name": "Person/managed/revise",
            "operation": "revise",
        },
        {"expectedVersion": 4, "kind": "thing", "name": "Person/managed/retract", "operation": "retract"},
    ]
    assert (summary.add_count, summary.revise_count, summary.retract_count, summary.preserved_count) == (1, 1, 1, 1)
    assert all(to_backend_stream_operation(row) == row for row in parse_jsonl(first))


def test_incomplete_scope_cannot_retract() -> None:
    with pytest.raises(KernelError, match="cannot authorize"):
        ScopePolicy("warmhub-data/example", "*/managed/**", False, "retract")


@pytest.mark.parametrize(
    "policy",
    [
        ("warmhub-data/example", "*/managed/**", True, "unknown"),
        ("warmhub-data/example", "*/managed/**", 1, "preserve"),
        ("warmhub-data@example", "*/managed/**", True, "preserve"),
    ],
)
def test_invalid_policy_fails_closed(policy: tuple[object, object, object, object]) -> None:
    with pytest.raises(KernelError):
        ScopePolicy(*policy)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "records",
    [
        ["not an object"],
        [{"kind": "thing", "wref": "Person/managed/no-data"}],
        [{"kind": "collection", "wref": "Arc/managed/no-members"}],
        [{"kind": "assertion", "wref": "Assignment/managed/no-about", "data": {}}],
    ],
)
def test_malformed_records_fail_closed(records: list[object]) -> None:
    with pytest.raises(KernelError):
        reconcile(records, [], ScopePolicy("warmhub-data/example", "*/managed/**", True, "preserve"))  # type: ignore[arg-type]


@pytest.mark.parametrize("version", [True, 0, -1])
def test_invalid_current_version_fails_closed(version: object) -> None:
    with pytest.raises(KernelError, match="positive integer version"):
        reconcile(
            [],
            [{**_thing("invalid-version", "Bad"), "active": True, "version": version}],
            ScopePolicy("warmhub-data/example", "*/managed/**", True, "preserve"),
        )


def test_relationship_endpoints_may_be_pinned() -> None:
    facility = "Facility/managed/example@v3"
    collection = {
        "kind": "collection",
        "shape": "Set",
        "wref": "Set/managed/example",
        "collectionType": "set",
        "members": [facility],
    }
    assertion = {
        "kind": "assertion",
        "shape": "Assignment",
        "wref": "Assignment/managed/example",
        "about": facility,
        "data": {"role": "member"},
    }

    operations, _ = reconcile(
        [collection, assertion],
        [],
        ScopePolicy("warmhub-data/example", "*/managed/**", True, "preserve"),
    )

    assert [row["name"] for row in parse_jsonl(operations)] == [
        "managed/example",
        assertion["wref"],
    ]
    assert all(to_backend_stream_operation(row) == row for row in parse_jsonl(operations))


def test_assertion_revision_omits_its_immutable_subject() -> None:
    desired = {
        "kind": "assertion",
        "shape": "Assignment",
        "wref": "Assignment/managed/example",
        "about": "Arc/managed/example",
        "data": {"role": "chair"},
    }
    current = {
        **desired,
        "about": "Arc/managed/example@v1",
        "active": True,
        "version": 2,
        "data": {"role": "member"},
    }

    operations, _ = reconcile(
        [desired],
        [current],
        ScopePolicy("warmhub-data/example", "*/managed/**", True, "preserve"),
    )

    assert parse_jsonl(operations) == [
        {
            "data": {"role": "chair"},
            "expectedVersion": 2,
            "kind": "assertion",
            "name": "Assignment/managed/example",
            "operation": "revise",
        }
    ]


def test_assertion_subject_changes_fail_closed() -> None:
    desired = {
        "kind": "assertion",
        "shape": "Assignment",
        "wref": "Assignment/managed/example",
        "about": "Arc/managed/next",
        "data": {"role": "member"},
    }
    current = {**desired, "about": "Arc/managed/prior@v1", "active": True, "version": 2}

    with pytest.raises(KernelError, match="subject is immutable"):
        reconcile(
            [desired],
            [current],
            ScopePolicy("warmhub-data/example", "*/managed/**", True, "preserve"),
        )


def test_collection_add_uses_bare_name_but_revise_and_retract_use_full_wrefs() -> None:
    revise = {
        "kind": "collection",
        "shape": "Arc",
        "wref": "Arc/managed/revise",
        "collectionType": "arc",
        "members": ["Person/managed/one", "Person/managed/three"],
    }
    retract = {
        "kind": "collection",
        "shape": "Arc",
        "wref": "Arc/managed/retract",
        "collectionType": "arc",
        "members": ["Person/managed/one", "Person/managed/two"],
    }

    operations, _ = reconcile(
        [revise],
        [
            {**revise, "active": True, "version": 2, "members": ["Person/managed/one", "Person/managed/two"]},
            {**retract, "active": True, "version": 3},
        ],
        ScopePolicy("warmhub-data/example", "*/managed/**", True, "retract"),
    )

    assert [(row["operation"], row["name"]) for row in parse_jsonl(operations)] == [
        ("revise", "Arc/managed/revise"),
        ("retract", "Arc/managed/retract"),
    ]


@pytest.mark.parametrize("reference", ["Facility/managed/example@v0", "Facility/managed/example@v01", "Facility/managed/example@v", "Facility/managed/example@v3@v4"])
def test_malformed_relationship_pins_fail_closed(reference: str) -> None:
    assertion = {
        "kind": "assertion",
        "shape": "Assignment",
        "wref": "Assignment/managed/example",
        "about": reference,
        "data": {},
    }
    with pytest.raises(KernelError, match="valid about reference"):
        reconcile([assertion], [], ScopePolicy("warmhub-data/example", "*/managed/**", True, "preserve"))


def test_shape_must_match_record_wref() -> None:
    record = {**_thing("shape", "Bad"), "shape": "Facility"}
    with pytest.raises(KernelError, match="shapes must match"):
        reconcile([record], [], ScopePolicy("warmhub-data/example", "*/managed/**", True, "preserve"))


def test_inactive_current_record_is_reactivated_with_add() -> None:
    current = _thing("return", "Old", version=3)
    current["active"] = False

    operations, summary = reconcile(
        [_thing("return", "New")],
        [current],
        ScopePolicy("warmhub-data/example", "*/managed/**", True, "retract"),
    )

    assert parse_jsonl(operations) == [
        {"data": {"label": "New"}, "kind": "thing", "name": "Person/managed/return", "operation": "add"}
    ]
    assert summary.add_count == 1


@pytest.mark.parametrize(
    ("desired", "current"),
    [
        ([_thing("duplicate", "One"), _thing("duplicate", "Two")], []),
        ([], [_thing("duplicate", "One", version=1), _thing("duplicate", "Two", version=2)]),
    ],
)
def test_duplicate_identities_fail(
    desired: list[dict[str, object]], current: list[dict[str, object]]
) -> None:
    with pytest.raises(KernelError, match="duplicate record identity"):
        reconcile(
            desired,
            current,
            ScopePolicy("warmhub-data/example", "*/managed/**", True, "preserve"),
        )


def test_state_outside_scope_is_preserved() -> None:
    outside = {**_thing("outside", "Keep", version=1), "wref": "Person/outside/keep"}

    operations, summary = reconcile(
        [],
        [outside],
        ScopePolicy("warmhub-data/example", "*/managed/**", True, "retract"),
    )

    assert operations == b""
    assert summary.preserved_count == 1


def test_incomplete_input_preserves_absent_state() -> None:
    operations, summary = reconcile(
        [],
        [_thing("keep", "Keep", version=1)],
        ScopePolicy("warmhub-data/example", "*/managed/**", False, "preserve"),
    )

    assert operations == b""
    assert summary.preserved_count == 1


def test_relationships_are_ordered_between_endpoint_changes() -> None:
    add_people = [_thing("add-a", "A"), _thing("add-b", "B")]
    add_arc = {
        "kind": "collection",
        "shape": "Arc",
        "wref": "Arc/managed/add",
        "collectionType": "arc",
        "members": [person["wref"] for person in add_people],
    }
    add_assertion = {
        "kind": "assertion",
        "shape": "Assignment",
        "wref": "Assignment/managed/add",
        "about": add_arc["wref"],
        "data": {"role": "member"},
    }
    retract_people = [_thing("retract-a", "A", version=1), _thing("retract-b", "B", version=1)]
    retract_arc = {
        "active": True,
        "version": 1,
        "kind": "collection",
        "shape": "Arc",
        "wref": "Arc/managed/retract",
        "collectionType": "arc",
        "members": [person["wref"] for person in retract_people],
    }
    retract_assertion = {
        "active": True,
        "version": 1,
        "kind": "assertion",
        "shape": "Assignment",
        "wref": "Assignment/managed/retract",
        "about": retract_arc["wref"],
        "data": {"role": "member"},
    }

    operations, _ = reconcile(
        [*add_people, add_arc, add_assertion],
        [*retract_people, retract_arc, retract_assertion],
        ScopePolicy("warmhub-data/example", "*/managed/**", True, "retract"),
    )

    assert [(row["operation"], row["kind"]) for row in parse_jsonl(operations)] == [
        ("add", "thing"),
        ("add", "thing"),
        ("add", "collection"),
        ("add", "assertion"),
        ("retract", "assertion"),
        ("retract", "collection"),
        ("retract", "thing"),
        ("retract", "thing"),
    ]


def test_artifact_store_verifies_published_bytes(tmp_path: Path) -> None:
    store = LocalArtifactStore(tmp_path)
    reference = store.persist(canonical_jsonl([_thing("one", "One")]))
    store.publish("desired", reference)

    resolved, payload = store.resolve("desired")

    assert resolved == reference
    assert json.loads(payload) == _thing("one", "One")


def test_artifact_store_does_not_copy_corrupt_bytes_to_scratch(tmp_path: Path) -> None:
    store = LocalArtifactStore(tmp_path)
    reference = store.persist(canonical_jsonl([_thing("one", "One")]))
    (tmp_path / reference.key).write_bytes(b"corrupt")
    target = tmp_path / "scratch.jsonl"

    with pytest.raises(KernelError, match="identity is inconsistent"):
        store.copy(reference, target)

    assert not target.exists()


def test_reconcile_streams_more_than_one_batch_and_ignores_input_order(tmp_path: Path) -> None:
    desired = [_thing(str(number), str(number)) for number in range(1001)]
    desired_path, current_path, first_path, second_path = (tmp_path / name for name in ("desired.jsonl", "current.jsonl", "first.jsonl", "second.jsonl"))
    desired_path.write_bytes(b"".join(canonical_jsonl([row]) for row in reversed(desired)))
    current_path.write_bytes(b"")
    first = reconcile_paths(desired_path, current_path, first_path, ScopePolicy("warmhub-data/example", "*/managed/**", True, "preserve"))
    desired_path.write_bytes(b"".join(canonical_jsonl([row]) for row in desired))
    second = reconcile_paths(desired_path, current_path, second_path, ScopePolicy("warmhub-data/example", "*/managed/**", True, "preserve"))
    assert first.summary.operation_count == 1001
    assert first_path.read_bytes() == second_path.read_bytes()
    assert first.sha256 == second.sha256


def test_reconcile_clears_a_previous_plan_when_inputs_are_invalid(tmp_path: Path) -> None:
    desired_path = tmp_path / "desired.jsonl"
    current_path = tmp_path / "current.jsonl"
    operations_path = tmp_path / "operations.jsonl"
    desired_path.write_bytes(b"not json\n")
    current_path.write_bytes(b"")
    operations_path.write_bytes(b'{"operation":"add"}\n')

    with pytest.raises(KernelError, match="invalid JSONL"):
        reconcile_paths(
            desired_path,
            current_path,
            operations_path,
            ScopePolicy("warmhub-data/example", "*/managed/**", True, "preserve"),
        )

    assert not operations_path.exists()


def test_reconcile_rejects_an_operation_path_that_aliases_an_input(tmp_path: Path) -> None:
    desired_path = tmp_path / "desired.jsonl"
    current_path = tmp_path / "current.jsonl"
    payload = canonical_jsonl([_thing("one", "One")])
    desired_path.write_bytes(payload)
    current_path.write_bytes(b"")

    with pytest.raises(KernelError, match="must not be an input"):
        reconcile_paths(
            desired_path,
            current_path,
            desired_path,
            ScopePolicy("warmhub-data/example", "*/managed/**", True, "preserve"),
        )

    assert desired_path.read_bytes() == payload


def test_additions_emit_referenced_records_before_their_referencers() -> None:
    # Observed on prod 2026-09-10: within the add phase, alphabetical order
    # put stat records ahead of the SourceArtifact addition they reference,
    # and WarmHub rejects forward references inside one commit.
    referencer = {
        "kind": "thing",
        "shape": "Measurement",
        "wref": "Measurement/managed/one",
        "data": {"evidence": "Source/managed/stream", "note": "x"},
    }
    referenced = {
        "kind": "thing",
        "shape": "Source",
        "wref": "Source/managed/stream",
        "data": {"label": "stream"},
    }
    policy = ScopePolicy("warmhub-data/example", "*/managed/**", False, "preserve")

    operations, _ = reconcile([referencer, referenced], [], policy)

    names = [row["name"] for row in parse_jsonl(operations)]
    assert names == ["Source/managed/stream", "Measurement/managed/one"]

    pinned = dict(referencer, data={"evidence": "Source/managed/stream@v3"})
    operations, _ = reconcile([pinned, referenced], [], policy)
    names = [row["name"] for row in parse_jsonl(operations)]
    assert names == ["Source/managed/stream", "Measurement/managed/one"]


def test_addition_reference_cycles_fail_closed() -> None:
    first = {
        "kind": "thing",
        "shape": "Person",
        "wref": "Person/managed/a",
        "data": {"peer": "Person/managed/b"},
    }
    second = {
        "kind": "thing",
        "shape": "Person",
        "wref": "Person/managed/b",
        "data": {"peer": "Person/managed/a"},
    }
    policy = ScopePolicy("warmhub-data/example", "*/managed/**", False, "preserve")

    with pytest.raises(KernelError, match="reference cycle"):
        reconcile([first, second], [], policy)
