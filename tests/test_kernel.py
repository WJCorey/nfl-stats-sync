from __future__ import annotations

import json
from tempfile import TemporaryDirectory
from pathlib import Path

import pytest
from warmhub.operations import to_backend_stream_operation

from mushroom.kernel import (
    KernelError, LocalArtifactStore, ScopePolicy, canonical_jsonl, matches_scope,
    parse_jsonl, reconcile as reconcile_paths,
)


def _thing(name: str, label: str, *, version: int | None = None) -> dict[str, object]:
    row: dict[str, object] = {"shapes": ["Person"], "name": f"managed/{name}", "data": {"label": label}}
    if version is not None:
        row.update(active=True, version=version)
    return row


_REVIEWED_TEST_FIELDS = {
    "Person": {}, "Facility": {}, "Station": {}, "Assignment": {}, "Tagged": {},
    "ForeignAssertionChild": {},
    "Observation": {"station": "wref"}, "Linked": {"next": "wref"},
}


def reconcile(desired, current, policy, **kwargs):
    kwargs.setdefault("reference_fields", _REVIEWED_TEST_FIELDS)
    with TemporaryDirectory() as directory:
        root = Path(directory)
        desired_path, current_path, operations_path = root / "desired.jsonl", root / "current.jsonl", root / "operations.jsonl"
        desired_path.write_bytes(b"".join(canonical_jsonl([row]) for row in desired))
        current_path.write_bytes(b"".join(canonical_jsonl([row], current=True) for row in current))
        result = reconcile_paths(desired_path, current_path, operations_path, policy, **kwargs)
        return operations_path.read_bytes(), result.summary


def policy(*, absence="preserve", complete=True, migration=False):
    return ScopePolicy("warmhub-data/example", "managed/**", complete, absence, allow_shape_changes=migration)


def test_managed_scope_matching_is_path_aware() -> None:
    assert matches_scope("example/alice", "example/**")
    assert not matches_scope("other/example/alice", "example/**")
    assert not matches_scope("example", "example/**")
    assert matches_scope("alice", "*")
    for pattern in ("*/{example,demo}/**", "*/exam*/**"):
        with pytest.raises(KernelError, match="only literal segments"):
            matches_scope("example/alice", pattern)


def test_reconcile_is_deterministic_and_complete() -> None:
    desired = [_thing("add", "new"), _thing("revise", "new label")]
    current = [_thing("retract", "gone", version=4), _thing("revise", "old label", version=2),
               {**_thing("outside", "keep", version=1), "name": "outside/keep"}]
    first, summary = reconcile(desired, current, policy(absence="retract"))
    second, _ = reconcile(list(reversed(desired)), list(reversed(current)), policy(absence="retract"))
    assert first == second
    assert parse_jsonl(first) == [
        {"data": {"label": "new"}, "shapes": ["Person"], "name": "managed/add", "operation": "add"},
        {"data": {"label": "new label"}, "expectedVersion": 2, "shapes": ["Person"], "name": "managed/revise", "operation": "revise"},
        {"expectedVersion": 4, "name": "managed/retract", "operation": "retract"},
    ]
    assert (summary.add_count, summary.revise_count, summary.retract_count, summary.preserved_count) == (1, 1, 1, 1)
    assert all(to_backend_stream_operation(row) == row for row in parse_jsonl(first))


def test_incomplete_scope_cannot_retract() -> None:
    with pytest.raises(KernelError, match="cannot authorize"):
        policy(absence="retract", complete=False)


@pytest.mark.parametrize("args", [
    ("warmhub-data/example", "managed/**", True, "unknown"),
    ("warmhub-data/example", "managed/**", 1, "preserve"),
    ("warmhub-data@example", "managed/**", True, "preserve"),
])
def test_invalid_policy_fails_closed(args):
    with pytest.raises(KernelError):
        ScopePolicy(*args)


@pytest.mark.parametrize("record", [
    "not an object", {"name":"managed/no-data", "shapes":[]},
    {"name":"managed/no-shapes", "data":{}},
    {"name":"managed/duplicate", "shapes":["Person", "Person"], "data":{}},
    {"name":"managed/legacy", "kind":"thing", "shapes":["Person"], "data":{}},
    {"name":"managed//empty", "shapes":[], "data":{}},
    {"name":"managed/pinned@v1", "shapes":[], "data":{}},
])
def test_malformed_records_fail_closed(record):
    with pytest.raises(KernelError):
        reconcile([record], [], policy())


@pytest.mark.parametrize("version", [True, 0, -1])
def test_invalid_current_version_fails_closed(version):
    with pytest.raises(KernelError, match="positive integer version"):
        reconcile([], [{**_thing("invalid-version", "Bad"), "active":True, "version":version}], policy())


def test_independent_names_multishape_and_shapeless_records():
    rows = [{"name":"managed/many", "shapes":["Facility", "Person"], "data":{}},
            {"name":"managed/none", "shapes":[], "data":{}}]
    operations, _ = reconcile(rows, [], policy())
    assert [row["shapes"] for row in parse_jsonl(operations)] == [["Facility", "Person"], []]


def test_declaration_order_is_neutral_and_certificates_are_not_declarations():
    desired = {**_thing("one", "One"), "shapes":["Person", "Facility"]}
    existing = {**desired, "shapes":["Facility", "Person"], "validatedShapes":["Thing", "Person", "Facility"], "referenceFields":{"Thing": {}, "Person": {}, "Facility": {}}, "active":True, "version":2}
    operations, summary = reconcile([desired], [existing], policy())
    assert operations == b""
    assert summary.unchanged_count == 1


def test_declaration_changes_require_explicit_migration():
    desired = {**_thing("one", "One"), "shapes":["Facility"]}
    existing = _thing("one", "One", version=2)
    with pytest.raises(KernelError, match="explicit migration"):
        reconcile([desired], [existing], policy())
    operations, _ = reconcile([desired], [existing], policy(migration=True))
    assert parse_jsonl(operations) == [{"operation":"revise", "name":"managed/one", "shapes":["Facility"], "data":{"label":"One"}, "expectedVersion":2}]


def test_relationship_endpoints_may_be_pinned():
    rows = [{"name":"managed/set", "shapes":["Set"], "data":{"members":["managed/facility@v3"]}},
            {"name":"managed/assignment", "shapes":["Assignment", "Assertion"], "data":{"about":"managed/facility@v3", "role":"member"}}]
    operations, _ = reconcile(rows, [], policy())
    assert len(parse_jsonl(operations)) == 2
    assert all(to_backend_stream_operation(row) == row for row in parse_jsonl(operations))


def test_assertion_revision_preserves_its_immutable_subject():
    desired = {"name":"managed/assignment", "shapes":["Assignment", "Assertion"], "data":{"about":"managed/arc", "role":"chair"}}
    current = {**desired, "active":True, "version":2, "data":{"about":"managed/arc@v1", "role":"member"}}
    operations, _ = reconcile([desired], [current], policy())
    assert parse_jsonl(operations) == [{"operation":"revise", "name":"managed/assignment", "shapes":["Assertion", "Assignment"], "data":{**desired["data"], "about":"managed/arc@v1"}, "expectedVersion":2}]


@pytest.mark.parametrize("about", ["managed/next", "managed/prior@v2", None])
def test_assertion_subject_changes_fail_closed(about):
    current = {"name":"managed/assignment", "shapes":["Assignment", "Assertion"], "active":True, "version":2, "data":{"about":"managed/prior@v1"}}
    desired = {"name":current["name"], "shapes":current["shapes"], "data":{} if about is None else {"about":about}}
    with pytest.raises(KernelError, match="subject is immutable|valid about reference"):
        reconcile([desired], [current], policy())


@pytest.mark.parametrize("reference", ["managed/example@v0", "managed/example@v01", "managed/example@v", "managed/example@v3@v4"])
def test_malformed_relationship_pins_fail_closed(reference):
    row = {"name":"managed/assignment", "shapes":["Assignment", "Assertion"], "data":{"about":reference}}
    with pytest.raises(KernelError, match="valid about reference"):
        reconcile([row], [], policy())


def test_collection_revise_preserves_all_body_fields():
    desired = {"name":"managed/arc", "shapes":["Arc", "Tagged"], "data":{"from":"managed/one", "to":"managed/three", "tag":"local"}}
    current = {**desired, "active":True, "version":2, "data":{**desired["data"], "to":"managed/two"}}
    operations, _ = reconcile([desired], [current], policy())
    assert parse_jsonl(operations)[0]["data"] == desired["data"]
    assert parse_jsonl(operations)[0]["shapes"] == ["Arc", "Tagged"]


def test_inactive_current_record_is_reactivated_with_add():
    current = {**_thing("return", "Old", version=3), "active":False}
    operations, summary = reconcile([_thing("return", "New")], [current], policy(absence="retract"))
    assert parse_jsonl(operations) == [{"data":{"label":"New"}, "shapes":["Person"], "name":"managed/return", "operation":"add"}]
    assert summary.add_count == 1


@pytest.mark.parametrize(("desired", "current"), [
    ([_thing("duplicate", "One"), _thing("duplicate", "Two")], []),
    ([], [_thing("duplicate", "One", version=1), _thing("duplicate", "Two", version=2)]),
])
def test_duplicate_identities_fail(desired, current):
    with pytest.raises(KernelError, match="duplicate record identity"):
        reconcile(desired, current, policy())


def test_state_outside_scope_is_preserved():
    outside = {**_thing("outside", "Keep", version=1), "name":"outside/keep"}
    operations, summary = reconcile([], [outside], policy(absence="retract"))
    assert operations == b""
    assert summary.preserved_count == 1


def test_desired_state_outside_scope_is_rejected():
    outside = {**_thing("outside", "Keep"), "name":"outside/keep"}
    with pytest.raises(KernelError, match="outside the managed scope"):
        reconcile([outside], [], policy())


def test_incomplete_input_preserves_absent_state():
    operations, summary = reconcile([], [_thing("keep", "Keep", version=1)], policy(complete=False))
    assert operations == b""
    assert summary.preserved_count == 1


def test_relationships_are_ordered_between_endpoint_changes():
    endpoints = [_thing("z-endpoint-a", "A"), _thing("z-endpoint-b", "B")]
    arc = {"name":"managed/b-arc", "shapes":["Arc"], "data":{"from":endpoints[0]["name"], "to":endpoints[1]["name"]}}
    assertion = {"name":"managed/a-assignment", "shapes":["Assignment", "Assertion"], "data":{"about":arc["name"]}}
    desired = [*endpoints, arc, assertion]
    operations, _ = reconcile(desired, [], policy())
    assert [row["name"] for row in parse_jsonl(operations)] == [*sorted(row["name"] for row in endpoints), arc["name"], assertion["name"]]
    operations, _ = reconcile([], [{**row, "active":True, "version":1} for row in desired], policy(absence="retract"))
    assert [row["name"] for row in parse_jsonl(operations)] == [assertion["name"], arc["name"], *sorted(row["name"] for row in endpoints)]


def test_typed_dependency_ordering_ignores_strings_and_handles_nested_fields():
    fields = {"Station": {}, "Observation":{"station":"wref", "details":{"links":["wref"]}}}
    station = {"name":"managed/z-station", "shapes":["Station"], "data":{"label":"managed/a-observation"}}
    observation = {"name":"managed/a-observation", "shapes":["Observation"], "data":{"station":station["name"], "details":{"links":[station["name"]]}}}
    operations, _ = reconcile([observation, station], [], policy(), reference_fields=fields)
    assert [row["name"] for row in parse_jsonl(operations)] == [station["name"], observation["name"]]


def test_revision_waits_for_referenced_add():
    fields = {"Station": {}, "Observation":{"station":"wref"}}
    station = {"name":"managed/z-station", "shapes":["Station"], "data":{}}
    observation = {"name":"managed/a-observation", "shapes":["Observation"], "data":{"station":station["name"]}}
    existing = {**observation, "active":True, "version":1, "data":{"station":"managed/prior"}}
    operations, _ = reconcile([observation, station], [existing], policy(), reference_fields=fields)
    assert [row["operation"] for row in parse_jsonl(operations)] == ["add", "revise"]


def test_dependency_cycles_fail_without_publishing_plan(tmp_path):
    rows = [{"name":"managed/a", "shapes":["Linked"], "data":{"next":"managed/b"}},
            {"name":"managed/b", "shapes":["Linked"], "data":{"next":"managed/a"}}]
    desired, current, output = [tmp_path / name for name in ("desired", "current", "output")]
    desired.write_bytes(canonical_jsonl(rows)); current.write_bytes(b"")
    with pytest.raises(KernelError, match="dependency cycle"):
        reconcile_paths(desired, current, output, policy(), reference_fields={"Linked":{"next":"wref"}})
    assert not output.exists()


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
    first = reconcile_paths(desired_path, current_path, first_path, ScopePolicy("warmhub-data/example", "managed/**", True, "preserve"), reference_fields={"Person": {}})
    desired_path.write_bytes(b"".join(canonical_jsonl([row]) for row in desired))
    second = reconcile_paths(desired_path, current_path, second_path, ScopePolicy("warmhub-data/example", "managed/**", True, "preserve"), reference_fields={"Person": {}})
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
            ScopePolicy("warmhub-data/example", "managed/**", True, "preserve"),
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
            ScopePolicy("warmhub-data/example", "managed/**", True, "preserve"),
        )

    assert desired_path.read_bytes() == payload


def test_cross_batch_fishkill_dependencies_precede_observations():
    station = {"name":"managed/z-station", "shapes":["Station"], "data":{}}
    observations = [{"name":f"managed/observation/{number:04}", "shapes":["Observation"],
                     "data":{"station":station["name"], "label":str(number)}} for number in range(1001)]
    operations, _ = reconcile([*observations, station], [], policy(), reference_fields={"Station": {}, "Observation":{"station":"wref"}})
    rows = parse_jsonl(operations)
    assert rows[0]["name"] == station["name"]
    assert len(rows) == 1002
    assert all(row["data"]["station"] == station["name"] for row in rows[1:])


def test_structural_pins_do_not_create_false_revisions():
    desired = {"name":"managed/arc", "shapes":["Arc"], "data":{"from":"managed/a", "to":"managed/b"}}
    current = {**desired, "active":True, "version":1, "data":{"from":"managed/a@v2", "to":"managed/b@v3"}}
    operations, summary = reconcile([desired], [current], policy())
    assert operations == b""
    assert summary.unchanged_count == 1
    pinned = {**desired, "data":{"from":"managed/a@v1", "to":"managed/b"}}
    operations, summary = reconcile([pinned], [current], policy())
    assert summary.revise_count == 1


def test_ordinary_about_field_is_mutable_text():
    desired = {**_thing("one", "One"), "data":{"about":"A new description"}}
    current = {**desired, "data":{"about":"The old description"}, "active":True, "version":1}
    operations, summary = reconcile([desired], [current], policy())
    assert summary.revise_count == 1


def test_inherited_assertion_subject_is_immutable():
    desired = {"name":"managed/assignment", "shapes":["Assignment"], "data":{"about":"managed/next"}}
    current = {**desired, "validatedShapes":["hub/shapes/Assertion@v1"], "active":True, "version":1, "data":{"about":"managed/prior@v1"}}
    with pytest.raises(KernelError, match="subject is immutable"):
        reconcile([desired], [current], policy(), reference_fields={"Assignment": {"about": "wref"}})


def test_shape_mutations_are_rejected():
    with pytest.raises(KernelError, match="Shape mutations"):
        reconcile([{"name":"hub/shapes/Person", "shapes":[], "data":{}}], [], ScopePolicy("warmhub-data/example", "**", True, "preserve"))


def test_local_canonical_reference_orders_adds_but_foreign_reference_does_not():
    station = {"name":"managed/z-station", "shapes":["Station"], "data":{}}
    observation = {"name":"managed/a-observation", "shapes":["Observation"], "data":{"station":"wh:warmhub-data/example/managed/z-station"}}
    fields = {"Station": {}, "Observation":{"station":"wref"}}
    operations, _ = reconcile([observation, station], [], policy(), reference_fields=fields)
    assert [row["name"] for row in parse_jsonl(operations)] == [station["name"], observation["name"]]
    foreign = {**observation, "data":{"station":"wh:warmhub-data/other/managed/z-station"}}
    operations, _ = reconcile([foreign, station], [], policy(), reference_fields=fields)
    assert [row["name"] for row in parse_jsonl(operations)] == [observation["name"], station["name"]]


def test_body_revision_preserves_equivalent_structural_pins():
    desired = {"name":"managed/arc", "shapes":["Arc", "Tagged"], "data":{"from":"managed/a", "to":"managed/b", "tag":"new"}}
    current = {**desired, "active":True, "version":1, "data":{"from":"managed/a@v2", "to":"managed/b@v3", "tag":"old"}}
    operations, _ = reconcile([desired], [current], policy())
    assert parse_jsonl(operations)[0]["data"] == {"from":"managed/a@v2", "to":"managed/b@v3", "tag":"new"}
    assert desired["data"]["from"] == "managed/a"


def test_foreign_shape_named_assertion_does_not_freeze_ordinary_about():
    desired = {"name": "managed/foreign", "shapes": ["ForeignAssertionChild"],
               "data": {"about": "updated text"}}
    current = {**desired, "active": True, "version": 1, "data": {"about": "prior text"},
               "validatedShapes": ["wh:other/repo/hub/shapes/Assertion@v1"], "referenceFields": {"wh:other/repo/hub/shapes/Assertion@v1": {}}}
    operations, summary = reconcile([desired], [current], policy())
    assert summary.revise_count == 1
    assert parse_jsonl(operations)[0]["data"] == desired["data"]


@pytest.mark.parametrize("shape,field,wanted,stored,unchanged", [
    ("Set", "members", ["managed/b", "managed/a", "managed/a"], ["managed/a@v1", "managed/b@v2"], True),
    ("Bond", "ends", ["managed/b", "managed/a"], ["managed/a@v1", "managed/b@v2"], True),
    ("List", "items", ["managed/b", "managed/a"], ["managed/a@v1", "managed/b@v2"], False),
    ("Set", "members", ["managed/a@v1"], ["managed/a@v2"], False),
    ("Bond", "ends", ["managed/a", "managed/a"], ["managed/a@v1", "managed/b@v2"], False),
])
def test_collection_semantics_preserve_equivalent_pins(shape, field, wanted, stored, unchanged):
    desired = {"name": "managed/group", "shapes": ["TaggedCollection"], "data": {field: wanted, "label": "old"}}
    current = {**desired, "active": True, "version": 1,
               "validatedShapes": [f"hub/shapes/{shape}@v1"], "data": {field: stored, "label": "old"}}
    operations, summary = reconcile([desired], [current], policy(), reference_fields={"TaggedCollection": {field: ["wref"]}})
    assert (summary.unchanged_count == 1) == unchanged
    if unchanged:
        assert operations == b""
        changed = {**desired, "data": {**desired["data"], "label": "new"}}
        operations, _ = reconcile([changed], [current], policy(), reference_fields={"TaggedCollection": {field: ["wref"]}})
        assert parse_jsonl(operations)[0]["data"][field] == stored
        assert changed["data"][field] == wanted


@pytest.mark.parametrize("binding", ["identity", "either"])
def test_actual_binding_does_not_reuse_historical_pin(binding):
    desired = {"name": "managed/observation", "shapes": ["Observation"], "data": {"station": "managed/station"}}
    current = {**desired, "active": True, "version": 1, "data": {"station": "managed/station@v1"},
               "validatedShapes": ["hub/shapes/Observation@v1"],
               "referenceFields": {"hub/shapes/Observation@v1": {"station": {"type": "wref", "binding": binding}}}}
    operations, summary = reconcile([desired], [current], policy(), reference_fields={"Observation": {"station": {"type": "wref", "binding": binding}}})
    assert summary.revise_count == 1
    assert parse_jsonl(operations)[0]["data"]["station"] == "managed/station"


def test_certified_set_either_contract_distinguishes_identity_and_version():
    schema = {"members": {"type": "array", "items": {"type": "wref", "binding": "either"}}}
    desired = {"name": "managed/set", "shapes": ["Set"], "data": {"members": ["managed/a@v1", "managed/a", "managed/a"]}}
    current = {**desired, "active": True, "version": 1, "data": {"members": ["managed/a", "managed/a@v1"]},
               "validatedShapes": ["hub/shapes/Set@v1"], "referenceFields": {"hub/shapes/Set@v1": schema}}
    operations, summary = reconcile([desired], [current], policy())
    assert summary.unchanged_count == 1
    assert operations == b""
    bare = {**desired, "data": {"members": ["managed/a"]}}
    pinned_current = {**current, "data": {"members": ["managed/a@v1"]}}
    operations, summary = reconcile([bare], [pinned_current], policy())
    assert summary.revise_count == 1
    assert parse_jsonl(operations)[0]["data"] == bare["data"]


@pytest.mark.parametrize("certified", [False, True])
def test_ordinary_nested_versioned_references_converge_and_survive_body_revisions(certified):
    schema = {"station": {"type": "wref", "binding": "versioned"},
              "nested": {"sources": [{"source": "wref?"}]}}
    desired = {"name": "managed/observation", "shapes": ["Observation"],
               "data": {"station": "managed/a@v1", "nested": {"sources": [{"source": "managed/b"}, {"source": None}]}, "label": "old"}}
    current = {**desired, "active": True, "version": 1,
               "data": {**desired["data"], "nested": {"sources": [{"source": "managed/b@v2"}, {"source": None}]}}}
    kwargs = {"reference_fields": {"Observation": schema}}
    if certified:
        current.update(validatedShapes=["hub/shapes/Observation@v1"], referenceFields={"hub/shapes/Observation@v1": schema})
    operations, summary = reconcile([desired], [current], policy(), **kwargs)
    assert summary.unchanged_count == 1
    assert operations == b""
    changed = {**desired, "data": {**desired["data"], "station": "managed/a", "label": "new"}}
    operations, _ = reconcile([changed], [current], policy(), **kwargs)
    assert parse_jsonl(operations)[0]["data"] == {**current["data"], "label": "new"}
    assert changed["data"]["station"] == "managed/a"
    explicit = {**desired, "data": {**desired["data"], "station": "managed/a@v2"}}
    operations, summary = reconcile([explicit], [current], policy(), **kwargs)
    assert summary.revise_count == 1
    assert parse_jsonl(operations)[0]["data"]["station"] == "managed/a@v2"


def test_declaration_revision_precedes_dependents_across_batches():
    station = {"name": "managed/z-station", "shapes": ["Station"], "data": {}}
    current = {**station, "shapes": ["LegacyStation"], "active": True, "version": 1}
    observations = [{"name": f"managed/observation/{number:04}", "shapes": ["Observation"],
                     "data": {"station": station["name"]}} for number in range(1001)]
    operations, summary = reconcile([*observations, station], [current], policy(migration=True),
                                    reference_fields={"Station": {}, "Observation": {"station": "wref"}})
    rows = parse_jsonl(operations)
    assert summary.revise_count == 1
    assert len(rows) == 1002
    assert rows[0]["operation"] == "revise"
    assert rows[0]["name"] == station["name"]


def test_body_only_revisions_do_not_create_dependency_cycles():
    desired = [{"name": f"managed/{name}", "shapes": ["Linked"], "data": {"next": f"managed/{other}", "label": "new"}}
               for name, other in [("a", "b"), ("b", "a")]]
    current = [{**record, "active": True, "version": 1, "data": {**record["data"], "label": "old"}} for record in desired]
    operations, summary = reconcile(desired, current, policy(), reference_fields={"Tagged": {}, "Linked": {"next": {"type": "wref", "binding": "identity"}}})
    assert summary.revise_count == 2
    assert len(parse_jsonl(operations)) == 2


def test_reference_fields_are_current_evidence_only():
    desired = {**_thing("one", "One"), "referenceFields": {"hub/shapes/Person@v1": {"label": "string"}}}
    with pytest.raises(KernelError, match="operation-neutral"):
        reconcile([desired], [], policy())
    current = {**desired, "active": True, "version": 1, "validatedShapes": []}
    with pytest.raises(KernelError, match="validated Shape certificates"):
        reconcile([], [current], policy())


def test_historical_reference_cycles_do_not_order_declaration_migrations():
    desired = [{"name": f"managed/{name}", "shapes": ["Linked", "Tagged"],
                "data": {"next": f"managed/{other}@v1"}} for name, other in [("a", "b"), ("b", "a")]]
    current = [{**record, "shapes": ["Linked"], "active": True, "version": 1} for record in desired]
    operations, summary = reconcile(desired, current, policy(migration=True), reference_fields={"Tagged": {}, "Linked": {"next": "wref"}})
    assert summary.revise_count == 2
    assert len(parse_jsonl(operations)) == 2


@pytest.mark.parametrize("shape,field", [("Set", "members"), ("Bond", "ends")])
def test_partial_unordered_change_preserves_moved_versioned_references(shape, field):
    desired = {"name": "managed/group", "shapes": [shape], "data": {field: ["managed/b", "managed/c"]}}
    current = {**desired, "active": True, "version": 1, "data": {field: ["managed/a@v1", "managed/b@v2"]},
               "validatedShapes": [f"hub/shapes/{shape}@v1"],
               "referenceFields": {f"hub/shapes/{shape}@v1": {field: {"type": "array", "items": {"type": "wref", "binding": "versioned"}}}}}
    operations, summary = reconcile([desired], [current], policy())
    assert summary.revise_count == 1
    assert parse_jsonl(operations)[0]["data"][field] == ["managed/b@v2", "managed/c"]
    assert desired["data"][field] == ["managed/b", "managed/c"]


def test_partial_either_set_change_preserves_requested_identity_binding():
    desired = {"name": "managed/group", "shapes": ["Set"], "data": {"members": ["managed/b", "managed/c"]}}
    current = {**desired, "active": True, "version": 1, "data": {"members": ["managed/a@v1", "managed/b@v2"]},
               "validatedShapes": ["hub/shapes/Set@v1"],
               "referenceFields": {"hub/shapes/Set@v1": {"members": {"type": "array", "items": {"type": "wref", "binding": "either"}}}}}
    operations, _ = reconcile([desired], [current], policy())
    assert parse_jsonl(operations)[0]["data"] == desired["data"]


def test_partial_unordered_change_requires_selector_for_ambiguous_history():
    desired = {"name": "managed/group", "shapes": ["Set"], "data": {"members": ["managed/b"]}}
    current = {**desired, "active": True, "version": 1, "data": {"members": ["managed/b@v1", "managed/b@v2"]}}
    with pytest.raises(KernelError, match="ambiguous historical collection reference"):
        reconcile([desired], [current], policy())
    explicit = {**desired, "data": {"members": ["managed/b@v2"]}}
    operations, _ = reconcile([explicit], [current], policy())
    assert parse_jsonl(operations)[0]["data"] == explicit["data"]


def test_unordered_match_reserves_explicit_selector_before_bare_name():
    desired = {"name": "managed/group", "shapes": ["Bond"], "data": {"ends": ["managed/b", "managed/b@v1"]}}
    current = {**desired, "active": True, "version": 1, "data": {"ends": ["managed/b@v1", "managed/b@v2"]}}
    operations, summary = reconcile([desired], [current], policy())
    assert summary.unchanged_count == 1
    assert operations == b""


def test_future_pinned_reference_depends_on_declaration_migration_across_batches():
    station = {"name": "managed/z-station", "shapes": ["Station"], "data": {}}
    current = {**station, "shapes": ["LegacyStation"], "active": True, "version": 4}
    observations = [{"name": f"managed/observation/{number:04}", "shapes": ["Observation"],
                     "data": {"station": "managed/z-station@v5"}} for number in range(1001)]
    operations, summary = reconcile([*observations, station], [current], policy(migration=True),
                                    reference_fields={"Station": {}, "Observation": {"station": "wref"}})
    rows = parse_jsonl(operations)
    assert summary.revise_count == 1
    assert len(rows) == 1002
    assert rows[0]["operation"] == "revise"
    assert rows[0]["name"] == station["name"]


def test_declaration_replacement_uses_reviewed_new_binding_contract():
    desired = {"name": "managed/observation", "shapes": ["NewObservation"], "data": {"station": "managed/station"}}
    current = {**desired, "shapes": ["OldObservation"], "active": True, "version": 1,
               "data": {"station": "managed/station@v1"}, "validatedShapes": ["hub/shapes/OldObservation@v1"],
               "referenceFields": {"hub/shapes/OldObservation@v1": {"station": {"type": "wref", "binding": "versioned"}}}}
    operations, _ = reconcile([desired], [current], policy(migration=True),
                              reference_fields={"NewObservation": {"station": {"type": "wref", "binding": "either"}}})
    assert parse_jsonl(operations)[0]["data"] == desired["data"]


def test_overlapping_nested_certificates_preserve_all_reference_fields():
    desired = {"name": "managed/observation", "shapes": ["WithStation", "WithOwner"],
               "data": {"nested": {"station": "managed/a", "owner": "managed/b"}, "label": "new"}}
    current = {**desired, "active": True, "version": 1,
               "data": {"nested": {"station": "managed/a@v1", "owner": "managed/b@v2"}, "label": "old"},
               "validatedShapes": ["hub/shapes/WithStation@v1", "hub/shapes/WithOwner@v1"],
               "referenceFields": {"hub/shapes/WithStation@v1": {"nested": {"station": "wref"}},
                                   "hub/shapes/WithOwner@v1": {"nested": {"owner": "wref"}}}}
    operations, _ = reconcile([desired], [current], policy(), reference_fields={"WithStation": {"nested": {"station": "wref"}}, "WithOwner": {"nested": {"owner": "wref"}}})
    assert parse_jsonl(operations)[0]["data"] == {**current["data"], "label": "new"}


def test_unknown_reference_bearing_replacement_fails_closed():
    desired = {"name": "managed/observation", "shapes": ["NewObservation"], "data": {"station": "managed/station"}}
    current = {**desired, "shapes": ["OldObservation"], "active": True, "version": 1,
               "data": {"station": "managed/station@v1"}, "validatedShapes": ["hub/shapes/OldObservation@v1"],
               "referenceFields": {"hub/shapes/OldObservation@v1": {"station": {"type": "wref", "binding": "versioned"}}}}
    with pytest.raises(KernelError, match="reviewed effective SHAPE_FIELDS"):
        reconcile([desired], [current], policy(migration=True))
    operations, _ = reconcile([desired], [current], policy(migration=True), reference_fields={"NewObservation": {}})
    assert parse_jsonl(operations)[0]["data"] == desired["data"]


@pytest.mark.parametrize("binding", ["versioned", "identity"])
def test_unordered_effective_binding_intersection_with_either(binding):
    desired_members = ["managed/b", "managed/c"]
    stored_members = ["managed/a@v1", "managed/b@v2"] if binding == "versioned" else ["managed/a", "managed/b"]
    desired = {"name": "managed/group", "shapes": ["RestrictedSet"], "data": {"members": desired_members}}
    current = {**desired, "active": True, "version": 1, "data": {"members": stored_members},
               "validatedShapes": ["hub/shapes/RestrictedSet@v1", "hub/shapes/Set@v1"],
               "referenceFields": {
                   "hub/shapes/RestrictedSet@v1": {"members": {"type": "array", "items": {"type": "wref", "binding": binding}}},
                   "hub/shapes/Set@v1": {"members": {"type": "array", "items": {"type": "wref", "binding": "either"}}}}}
    operations, _ = reconcile([desired], [current], policy(), reference_fields={"RestrictedSet": {"members": {"type": "array", "items": {"type": "wref", "binding": binding}}}})
    expected = ["managed/b@v2", "managed/c"] if binding == "versioned" else desired_members
    assert parse_jsonl(operations)[0]["data"]["members"] == expected


def test_retired_inherited_reference_contract_requires_all_desired_contracts():
    desired = {"name": "managed/observation", "shapes": ["NewObservation", "Tagged"],
               "data": {"station": "managed/station", "label": "new"}}
    current = {**desired, "shapes": ["OldObservation", "Tagged"], "active": True, "version": 1,
               "data": {"station": "managed/station@v1", "label": "old"},
               "validatedShapes": ["hub/shapes/OldObservation@v1", "hub/shapes/BaseObservation@v1", "hub/shapes/Tagged@v1"],
               "referenceFields": {"hub/shapes/OldObservation@v1": {},
                                   "hub/shapes/BaseObservation@v1": {"station": "wref"},
                                   "hub/shapes/Tagged@v1": {"label": "string"}}}
    contracts = {"NewObservation": {"station": {"type": "wref", "binding": "either"}}}
    with pytest.raises(KernelError, match="reviewed effective SHAPE_FIELDS"):
        reconcile([desired], [current], policy(migration=True), reference_fields=contracts)
    operations, _ = reconcile([desired], [current], policy(migration=True),
                              reference_fields={**contracts, "Tagged": {"label": "string"}})
    assert parse_jsonl(operations)[0]["data"] == desired["data"]


def test_body_revision_precedes_reference_to_its_next_version():
    target = _thing("z-target", "new")
    source = {"name": "managed/a-source", "shapes": ["Linked"], "data": {"next": "managed/z-target@v2"}}
    operations, _ = reconcile([source, target], [_thing("z-target", "old", version=1)], policy(),
                              reference_fields={"Person": {}, "Linked": {"next": "wref"}})
    assert [(row["name"], row["operation"]) for row in parse_jsonl(operations)] == [
        (target["name"], "revise"), (source["name"], "add")]


@pytest.mark.parametrize("source_state", ["add", "unchanged", "preserved"])
def test_surviving_floating_reference_prevents_target_retraction(source_state):
    source = {"name": "managed/source", "shapes": ["Linked"], "data": {"next": "managed/target"}}
    desired = [source] if source_state != "preserved" else []
    current = [_thing("target", "target", version=1)]
    if source_state != "add":
        source = {**source, "name": "outside/source" if source_state == "preserved" else source["name"]}
        current.append({**source, "active": True, "version": 1})
    with pytest.raises(KernelError, match="references a scheduled retraction"):
        reconcile(desired, current, policy(absence="retract"), reference_fields={"Tagged": {}, "Linked": {"next": {"type": "wref", "binding": "identity"}}})


def test_surviving_historical_pin_allows_target_retraction():
    source = {"name": "managed/source", "shapes": ["Linked"], "data": {"next": "managed/target@v1"}}
    operations, _ = reconcile([source], [_thing("target", "target", version=1)], policy(absence="retract"),
                              reference_fields={"Tagged": {}, "Linked": {"next": "wref"}})
    assert [row["operation"] for row in parse_jsonl(operations)] == ["add", "retract"]


@pytest.mark.parametrize("pinned", [False, True])
def test_retraction_cycles_only_require_order_for_floating_references(pinned):
    current = [{"name": f"managed/{name}", "shapes": ["Linked"], "active": True, "version": 1,
                "data": {"next": f"managed/{other}" + ("@v1" if pinned else "")}}
               for name, other in [("a", "b"), ("b", "a")]]
    if not pinned:
        with pytest.raises(KernelError, match="dependency cycle"):
            reconcile([], current, policy(absence="retract"), reference_fields={"Tagged": {}, "Linked": {"next": "wref"}})
    else:
        operations, _ = reconcile([], current, policy(absence="retract"), reference_fields={"Tagged": {}, "Linked": {"next": "wref"}})
        assert [row["operation"] for row in parse_jsonl(operations)] == ["retract", "retract"]


def test_existing_self_reference_allows_declaration_migration():
    desired = {"name": "managed/a", "shapes": ["Linked", "Tagged"], "data": {"next": "managed/a@v1"}}
    current = {**desired, "shapes": ["Linked"], "active": True, "version": 1}
    operations, _ = reconcile([desired], [current], policy(migration=True), reference_fields={"Tagged": {}, "Linked": {"next": "wref"}})
    assert parse_jsonl(operations)[0]["operation"] == "revise"


def test_reactivation_declaration_change_requires_explicit_migration():
    desired = {**_thing("one", "One"), "shapes": ["Facility"]}
    current = {**_thing("one", "One", version=2), "active": False}
    with pytest.raises(KernelError, match="explicit migration"):
        reconcile([desired], [current], policy())
    operations, _ = reconcile([desired], [current], policy(migration=True))
    assert parse_jsonl(operations)[0]["operation"] == "add"


def test_retired_null_reference_contract_does_not_apply_to_new_free_text_field():
    desired = {"name": "managed/a", "shapes": ["New"], "data": {"parent": "free text"}}
    current = {**desired, "shapes": ["Old"], "active": True, "version": 1, "data": {"parent": None},
               "validatedShapes": ["hub/shapes/Old@v1"], "referenceFields": {"hub/shapes/Old@v1": {"parent": "wref?"}}}
    with pytest.raises(KernelError, match="reviewed effective SHAPE_FIELDS"):
        reconcile([desired], [current], policy(migration=True))
    operations, _ = reconcile([desired], [current], policy(migration=True), reference_fields={"New": {"parent": "string"}})
    assert parse_jsonl(operations)[0]["data"] == desired["data"]


def test_sdk_write_refusal_is_reported_as_kernel_error(monkeypatch):
    from warmhub.errors import WriteOperationError

    def refuse(operation):
        raise WriteOperationError(None, "bad operation")

    monkeypatch.setattr("mushroom.kernel.to_backend_stream_operation", refuse)
    with pytest.raises(KernelError, match="rejected by the pinned WarmHub SDK") as error:
        reconcile([_thing("one", "One")], [], policy())
    assert isinstance(error.value.__cause__, WriteOperationError)


def test_existing_identity_self_reference_allows_declaration_migration():
    desired = {"name": "managed/a", "shapes": ["Linked", "Tagged"], "data": {"next": "managed/a"}}
    current = {**desired, "shapes": ["Linked"], "active": True, "version": 1}
    operations, _ = reconcile([desired], [current], policy(migration=True),
                              reference_fields={"Tagged": {}, "Linked": {"next": {"type": "wref", "binding": "identity"}}})
    assert parse_jsonl(operations)[0]["operation"] == "revise"


@pytest.mark.parametrize("contracts", [None, {}, {"Unknown": None}])
def test_every_custom_desired_declaration_requires_an_effective_contract(tmp_path, contracts):
    desired_path, current_path, output = [tmp_path / name for name in ("desired", "current", "output")]
    desired_path.write_bytes(canonical_jsonl([{"name": "managed/a", "shapes": ["Unknown"], "data": {}}]))
    current_path.write_bytes(b"")
    output.write_bytes(b"old plan\n")
    with pytest.raises(KernelError, match="reviewed effective SHAPE_FIELDS"):
        reconcile_paths(desired_path, current_path, output, policy(), reference_fields=contracts)
    assert not output.exists()


def test_explicit_empty_custom_contract_is_reference_free():
    desired = {"name": "managed/a", "shapes": ["ReviewedText"], "data": {"parent": "managed/b"}}
    operations, _ = reconcile([desired], [], policy(), reference_fields={"ReviewedText": {}})
    assert parse_jsonl(operations)[0]["data"] == desired["data"]


def test_existing_certificate_does_not_authorize_a_new_desired_contract():
    desired = {"name": "managed/a", "shapes": ["Unknown"], "data": {"parent": "managed/b"}}
    current = {**desired, "active": True, "version": 1, "validatedShapes": ["hub/shapes/Unknown@v1"],
               "referenceFields": {"hub/shapes/Unknown@v1": {"parent": "wref"}}}
    with pytest.raises(KernelError, match="reviewed effective SHAPE_FIELDS"):
        reconcile([desired], [current], policy(), reference_fields={})


def test_added_reference_bearing_declaration_requires_reviewed_effective_contract():
    desired = {**_thing("a", "A"), "shapes": ["Person", "NewReferences"], "data": {"parent": "managed/b"}}
    with pytest.raises(KernelError, match="NewReferences"):
        reconcile([desired], [_thing("a", "A", version=1)], policy(migration=True), reference_fields={"Person": {}})


def test_composed_assertion_add_uses_reviewed_effective_about_contract():
    endpoint = _thing("z-target", "Z")
    assertion = {"name": "managed/a-assertion", "shapes": ["SemanticContract"],
                 "data": {"about": endpoint["name"], "claim": "verified"}}
    operations, _ = reconcile([assertion, endpoint], [], policy(),
                              reference_fields={"Person": {}, "SemanticContract": {"about": "wref"}})
    assert [operation["name"] for operation in parse_jsonl(operations)] == [endpoint["name"], assertion["name"]]


def test_legacy_indexed_assertion_representation_change_is_a_noop():
    desired = {"name": "managed/assertion", "shapes": ["Assertion"],
               "data": {"about": "managed/subject", "claim": "verified"}}
    current = {**desired, "active": True, "version": 2, "data": {**desired["data"], "about": "managed/subject@v1"},
               "assertionSubject": "managed/subject@v1"}
    operations, summary = reconcile([desired], [current], policy())
    assert operations == b""
    assert summary.unchanged_count == 1


def test_legacy_indexed_subject_is_immutable_without_assertion_certificate():
    desired = {"name": "managed/assertion", "shapes": ["SemanticContract"],
               "data": {"about": "managed/new", "claim": "verified"}}
    current = {**desired, "active": True, "version": 2, "data": {**desired["data"], "about": "managed/prior@v1"},
               "assertionSubject": "managed/prior@v1"}
    with pytest.raises(KernelError, match="subject is immutable"):
        reconcile([desired], [current], policy(), reference_fields={"SemanticContract": {"about": "wref"}})


def test_indexed_assertion_evidence_orders_legacy_retractions():
    endpoint = _thing("a-target", "A", version=1)
    assertion = {"name": "managed/z-assertion", "shapes": ["SemanticContract"], "active": True, "version": 2,
                 "data": {"about": endpoint["name"]}, "assertionSubject": endpoint["name"]}
    operations, _ = reconcile([], [endpoint, assertion], policy(absence="retract"), reference_fields={})
    assert [operation["name"] for operation in parse_jsonl(operations)] == [assertion["name"], endpoint["name"]]


@pytest.mark.parametrize("subject", ["managed/other@v1", None, "managed/a@v0"])
def test_assertion_subject_evidence_must_match_normalized_data(subject):
    current = {**_thing("assertion", "A", version=1), "data": {"about": "managed/a@v1"}, "assertionSubject": subject}
    with pytest.raises(KernelError, match="assertionSubject"):
        reconcile([], [current], policy())


def test_assertion_subject_is_current_evidence_only():
    desired = {**_thing("assertion", "A"), "data": {"about": "managed/a@v1"}, "assertionSubject": "managed/a@v1"}
    with pytest.raises(KernelError, match="operation-neutral"):
        reconcile([desired], [], policy())


@pytest.mark.parametrize("override", [{}, {"members": ["string"]}, {"members": ["wref"]}])
def test_builtin_contracts_cannot_be_overridden(override):
    desired = {"name": "managed/set", "shapes": ["Set"], "data": {"members": []}}
    with pytest.raises(KernelError, match="cannot override builtin"):
        reconcile([desired], [], policy(), reference_fields={"Set": override})


def test_content_uses_builtin_reference_free_contract():
    desired = {"name": "managed/readme", "shapes": ["Content"], "data": {"text": "managed/station"}}
    operations, _ = reconcile([desired], [], policy(), reference_fields={})
    assert parse_jsonl(operations)[0]["data"] == desired["data"]


@pytest.mark.parametrize("observed", [{}, {"Reference@v1": {"parent": {"type": "wref", "binding": "identity"}}}])
@pytest.mark.parametrize("migration", [False, True])
def test_retained_declaration_requires_complete_matching_certified_bindings(observed, migration):
    desired = {"name": "managed/a", "shapes": ["Reference"], "data": {"parent": "managed/b", "label": "new"}}
    current = {**desired, "active": True, "version": 1, "validatedShapes": ["Reference@v1"],
               "referenceFields": observed, "data": {**desired["data"], "label": "old"}}
    with pytest.raises(KernelError, match="reviewed schema migration"):
        reconcile([desired], [current], policy(migration=migration), reference_fields={"Reference": {"parent": "wref"}})


def test_declaration_migration_does_not_approve_another_rows_missing_reference_contract():
    desired = {"name": "managed/observation", "shapes": ["Observation"], "data": {"station": "managed/station"}}
    current = {**desired, "active": True, "version": 1, "data": {"station": "managed/station@v3"},
               "validatedShapes": ["Observation@v1"], "referenceFields": {"Observation@v1": {"station": "wref"}}}
    migrated = {**_thing("person", "A"), "shapes": ["Person", "Tagged"]}
    with pytest.raises(KernelError, match="observed reference bindings differ"):
        reconcile([desired, migrated], [current, _thing("person", "A", version=1)], policy(migration=True),
                  reference_fields={"Observation": {}, "Person": {}, "Tagged": {}})


@pytest.mark.parametrize("old_binding,new_binding", [
    ("versioned", "either"), ("versioned", "identity"),
    ("either", "versioned"), ("either", "identity"),
    ("identity", "versioned"), ("identity", "either"),
])
def test_retained_reference_binding_changes_require_row_migration(old_binding, new_binding):
    desired = {"name": "managed/reference", "shapes": ["Reference"], "data": {"parent": "managed/target"}}
    current = {**desired, "active": True, "version": 1, "validatedShapes": ["Reference@v1"],
               "referenceFields": {"Reference@v1": {"parent": {"type": "wref", "binding": old_binding}}}}
    with pytest.raises(KernelError, match="observed reference bindings differ"):
        reconcile([desired], [current], policy(migration=True),
                  reference_fields={"Reference": {"parent": {"type": "wref", "binding": new_binding}}})


@pytest.mark.parametrize("body_changes", [False, True])
def test_additive_optional_reference_contract_accepts_mixed_certificate_versions(body_changes):
    old_fields = {"station": "wref", "label": "string"}
    new_fields = {**old_fields, "buoy?": "wref"}
    desired = [{"name": f"managed/observation-{version}", "shapes": ["Observation"],
                "data": {"station": "managed/station", "label": "new" if body_changes else "old"}}
               for version in (1, 2)]
    current = [{**row, "active": True, "version": 3,
                "data": {"station": "managed/station@v2", "label": "old"},
                "validatedShapes": [f"Observation@v{version}"],
                "referenceFields": {f"Observation@v{version}": old_fields if version == 1 else new_fields}}
               for version, row in enumerate(desired, 1)]
    operations, summary = reconcile(desired, current, policy(), reference_fields={"Observation": new_fields})
    if body_changes:
        assert summary.revise_count == 2
        assert all(operation["data"] == {"station": "managed/station@v2", "label": "new"}
                   for operation in parse_jsonl(operations))
    else:
        assert operations == b""
        assert summary.unchanged_count == 2


def test_added_declaration_approves_binding_change_for_that_row():
    desired = {"name": "managed/reference", "shapes": ["Reference", "Tagged"], "data": {"parent": "managed/target"}}
    current = {**desired, "shapes": ["Reference"], "active": True, "version": 1,
               "data": {"parent": "managed/target@v3"}, "validatedShapes": ["Reference@v1"],
               "referenceFields": {"Reference@v1": {"parent": "wref"}}}
    operations, summary = reconcile([desired], [current], policy(migration=True),
                                   reference_fields={"Reference": {"parent": {"type": "wref", "binding": "identity"}}, "Tagged": {}})
    assert summary.revise_count == 1
    assert parse_jsonl(operations)[0]["data"] == desired["data"]


def test_composed_assertion_adopts_fieldless_certificate_without_declaration_migration():
    desired = {"name": "managed/assertion", "shapes": ["SemanticContract"],
               "data": {"about": "managed/subject", "claim": "verified"}}
    current = {**desired, "active": True, "version": 2, "data": {**desired["data"], "about": "managed/subject@v1"},
               "assertionSubject": "managed/subject@v1", "needsAssertionAdoption": True,
               "validatedShapes": ["Assertion@v1", "SemanticContract@v1"],
               "referenceFields": {"Assertion@v1": {}, "SemanticContract@v1": {"claim": "string"}}}
    operations, summary = reconcile([desired], [current], policy(),
                                   reference_fields={"SemanticContract": {"about": "wref", "claim": "string"}})
    assert summary.revise_count == 1
    assert parse_jsonl(operations)[0]["data"]["about"] == "managed/subject@v1"


def test_effective_nested_binding_intersection_matches_certified_closure():
    desired = {"name": "managed/a", "shapes": ["Child"], "data": {"nested": {"parents": ["managed/b"]}, "label": "new"}}
    current = {**desired, "active": True, "version": 1, "validatedShapes": ["Child@v1", "Base@v1"],
               "referenceFields": {"Child@v1": {"nested": {"parents": [{"type": "wref", "binding": "versioned"}]}},
                                   "Base@v1": {"nested": {"parents": [{"type": "wref", "binding": "either"}]}}},
               "data": {"nested": {"parents": ["managed/b@v1"]}, "label": "old"}}
    operations, _ = reconcile([desired], [current], policy(), reference_fields={"Child": {"nested": {"parents": ["wref"]}}})
    assert parse_jsonl(operations)[0]["data"]["nested"] == current["data"]["nested"]


@pytest.mark.parametrize("binding,expected", [
    ("versioned", ["managed/z-target", "managed/a-source"]),
    ("identity", ["managed/a-source", "managed/z-target"]),
    ("either", ["managed/a-source", "managed/z-target"]),
])
def test_new_floating_reference_orders_body_revision_only_for_versioned_binding(binding, expected):
    source = {"name": "managed/a-source", "shapes": ["Reference"], "data": {"parent": "managed/z-target"}}
    target = _thing("z-target", "new")
    operations, _ = reconcile([source, target], [_thing("z-target", "old", version=1)], policy(),
                              reference_fields={"Person": {}, "Reference": {"parent": {"type": "wref", "binding": binding}}})
    assert [row["name"] for row in parse_jsonl(operations)] == expected


def test_new_floating_versioned_references_cannot_plan_mutual_next_version_cycle():
    desired = [{"name": f"managed/{name}", "shapes": ["Reference"], "data": {"parent": f"managed/{other}", "label": "new"}}
               for name, other in [("a", "b"), ("b", "a")]]
    current = [{**row, "active": True, "version": 1, "data": {"label": "old"}} for row in desired]
    with pytest.raises(KernelError, match="dependency cycle"):
        reconcile(desired, current, policy(), reference_fields={"Reference": {"parent?": "wref"}})


def test_self_reference_does_not_prevent_retraction():
    current = {"name": "managed/a", "shapes": ["Reference"], "active": True, "version": 1, "data": {"parent": "managed/a"}}
    operations, _ = reconcile([], [current], policy(absence="retract"), reference_fields={"Reference": {"parent": "wref"}})
    assert parse_jsonl(operations) == [{"operation": "retract", "name": "managed/a", "expectedVersion": 1}]


@pytest.mark.parametrize("declarations,certificates", [(["Station"], []), (["StationChild"], ["hub/shapes/Station@v1"])])
def test_hint_style_identity_cannot_duplicate_existing_bare_name(declarations, certificates):
    desired = {"name": "Station/41008", "shapes": ["Station"], "data": {}}
    current = {"name": "41008", "shapes": declarations, "active": True, "version": 1, "data": {}, "validatedShapes": certificates}
    with pytest.raises(KernelError, match="duplicate active bare name"):
        reconcile([desired], [current], ScopePolicy("warmhub-data/example", "**", True, "preserve"), reference_fields={"Station": {}})


def test_shape_named_namespace_without_colliding_bare_identity_is_allowed():
    desired = {"name": "Station/41008", "shapes": ["Station"], "data": {}}
    operations, _ = reconcile([desired], [], ScopePolicy("warmhub-data/example", "**", True, "preserve"), reference_fields={"Station": {}})
    assert parse_jsonl(operations)[0]["name"] == desired["name"]


@pytest.mark.parametrize("current", [[], [{**_thing("a", "A", version=1), "active": False}],
                                     [{**_thing("a", "A", version=1), "name": "outside/a"}]])
def test_migration_guard_requires_active_managed_current_records(current):
    guarded = ScopePolicy("warmhub-data/example", "managed/**", True, "preserve", require_existing_records=True)
    with pytest.raises(KernelError, match="requires existing active records"):
        reconcile([_thing("a", "A")], current, guarded)


def test_existing_record_guard_accepts_noop_migration_and_validates_boolean():
    guarded = ScopePolicy("warmhub-data/example", "managed/**", True, "preserve", require_existing_records=True)
    operations, summary = reconcile([_thing("a", "A")], [_thing("a", "A", version=1)], guarded)
    assert operations == b""
    assert summary.unchanged_count == 1
    with pytest.raises(KernelError, match="require_existing_records must be a boolean"):
        ScopePolicy("warmhub-data/example", "managed/**", True, "preserve", require_existing_records=1)


def test_indexed_only_assertion_requires_one_native_adoption_revision():
    desired = {"name": "managed/assertion", "shapes": ["Assertion"], "data": {"about": "managed/subject", "claim": "verified"}}
    current = {**desired, "active": True, "version": 2, "data": {**desired["data"], "about": "managed/subject@v1"},
               "assertionSubject": "managed/subject@v1", "needsAssertionAdoption": True,
               "validatedShapes": ["Assertion@v1"], "referenceFields": {"Assertion@v1": {}}}
    operations, summary = reconcile([desired], [current], policy())
    assert summary.revise_count == 1
    operation = parse_jsonl(operations)[0]
    assert operation["data"]["about"] == "managed/subject@v1"
    assert operation["expectedVersion"] == 2
    native = {**current, "version": 3, "data": operation["data"]}
    native.pop("needsAssertionAdoption")
    residual, summary = reconcile([desired], [native], policy())
    assert residual == b""
    assert summary.unchanged_count == 1


@pytest.mark.parametrize("value", [1, "true", None])
def test_assertion_adoption_marker_must_be_boolean(value):
    current = {**_thing("a", "A", version=1), "needsAssertionAdoption": value}
    with pytest.raises(KernelError, match="needsAssertionAdoption"):
        reconcile([], [current], policy())


def test_assertion_adoption_marker_requires_indexed_evidence():
    current = {**_thing("a", "A", version=1), "needsAssertionAdoption": True}
    with pytest.raises(KernelError, match="indexed assertionSubject evidence"):
        reconcile([], [current], policy())


def test_existing_floating_versioned_self_reference_allows_body_revision():
    desired = {"name": "managed/a", "shapes": ["Reference"], "data": {"parent": "managed/a", "label": "new"}}
    current = {**desired, "active": True, "version": 1, "data": {**desired["data"], "label": "old"}}
    operations, _ = reconcile([desired], [current], policy(), reference_fields={"Reference": {"parent": "wref"}})
    assert parse_jsonl(operations)[0]["operation"] == "revise"


def test_future_versioned_self_reference_still_requires_impossible_order():
    desired = {"name": "managed/a", "shapes": ["Reference"], "data": {"parent": "managed/a@v2", "label": "new"}}
    current = {**desired, "active": True, "version": 1, "data": {"label": "old"}}
    with pytest.raises(KernelError, match="dependency cycle"):
        reconcile([desired], [current], policy(), reference_fields={"Reference": {"parent?": "wref"}})


def test_assertion_adoption_marker_is_current_only():
    desired = {**_thing("assertion", "A"), "needsAssertionAdoption": True}
    with pytest.raises(KernelError, match="operation-neutral"):
        reconcile([desired], [], policy())


def test_hint_guard_preserves_existing_explicit_shape_named_namespace():
    desired = {"name": "Station/41008", "shapes": ["Station"], "data": {}}
    current = [{**desired, "active": True, "version": 1},
               {**desired, "name": "41008", "active": True, "version": 2}]
    operations, summary = reconcile([desired], current, ScopePolicy("warmhub-data/example", "**", True, "preserve"),
                                    reference_fields={"Station": {}})
    assert operations == b""
    assert summary.unchanged_count == 1
    assert summary.preserved_count == 1
