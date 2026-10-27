from copy import deepcopy

import pytest

from mushroom.kernel import ScopePolicy, canonical_jsonl, parse_jsonl, reconcile
from mushroom.references import _is_type_spec_object, map_typed_references


@pytest.mark.parametrize("schema,expected", [
    ({"type": "wref", "binding": "string"}, False),
    ({"type": "string", "enum": "wref"}, False),
    ({"type": "string", "minLength": "wref"}, False),
    ({"type": "number", "integer": "wref"}, False),
    ({"type": "array", "items": "wref"}, True),
    ({"type": "array", "items": 1}, False),
    ({"type": "array", "items": True}, False),
    ({"type": "wref", "shape": {"name": "string"}}, False),
    ({"type": "wref?", "binding": "versioned"}, True),
    ({"type": "string", "enum": ["wref"]}, True),
])
def test_schema_classifier_distinguishes_nested_fields_from_constraints(schema, expected):
    assert _is_type_spec_object(schema) is expected


@pytest.mark.parametrize("schema", [["wref"], {"type": "array", "items": {"type": "wref", "binding": "versioned"}}])
@pytest.mark.parametrize("desired,expected", [
    (["a", "b", "c"], ["a@v1", "b", "c@v3"]),
    (["c", "a"], ["c@v3", "a@v1"]),
    (["c", "c", "a@v4"], ["c@v3", "c@v3", "a@v4"]),
])
def test_reference_array_preserves_pins_by_identity_without_reordering(schema, desired, expected):
    assert map_typed_references(deepcopy(desired), schema, lambda ref: ref, current=["a@v1", "c@v3"]) == expected


def test_reference_array_requires_explicit_selector_when_history_is_ambiguous():
    with pytest.raises(ValueError, match="ambiguous historical array reference"):
        map_typed_references(["a"], ["wref"], lambda ref: ref, current=["a@v1", "a@v2"])
    assert map_typed_references(["a@v2", "a@v1"], ["wref"], lambda ref: ref, current=["a@v1", "a@v2"]) == ["a@v2", "a@v1"]


@pytest.mark.parametrize("binding", ["identity", "either"])
def test_reference_array_does_not_preserve_pins_for_other_bindings(binding):
    assert map_typed_references(["a"], [{"type": "wref", "binding": binding}], lambda ref: ref, current=["a@v1", "a@v2"]) == ["a"]


@pytest.mark.parametrize("items,expected", [
    (["managed/a", "managed/b", "managed/c"], ["managed/a@v1", "managed/b", "managed/c@v3"]),
    (["managed/c", "managed/a"], ["managed/c@v3", "managed/a@v1"]),
])
def test_list_revise_payload_keeps_retained_item_versions(tmp_path, items, expected):
    desired = {"name": "managed/list", "shapes": ["List"], "data": {"items": items}}
    current = {**desired, "active": True, "version": 2, "data": {"items": ["managed/a@v1", "managed/c@v3"]}}
    desired_path, current_path, operations_path = (tmp_path / name for name in ("desired.jsonl", "current.jsonl", "operations.jsonl"))
    desired_path.write_bytes(canonical_jsonl([desired]))
    current_path.write_bytes(canonical_jsonl([current], current=True))
    reconcile(desired_path, current_path, operations_path, ScopePolicy("warmhub-data/example", "managed/**", True, "preserve"))
    assert parse_jsonl(operations_path.read_bytes())[0]["data"]["items"] == expected


@pytest.mark.parametrize("fields", [{"type": "wref"}, {"type": "string", "enum": ["wref"]}])
def test_root_field_maps_keep_references_during_body_revisions(tmp_path, fields):
    field = "type" if fields["type"] == "wref" else "enum"
    reference = "managed/station" if field == "type" else ["managed/station"]
    historical = "managed/station@v2" if field == "type" else ["managed/station@v2"]
    desired = {"name": "managed/record", "shapes": ["Example"], "data": {field: reference, "label": "new"}}
    current = {**desired, "active": True, "version": 1, "data": {field: historical, "label": "old"},
               "validatedShapes": ["Example@v1"], "referenceFields": {"Example@v1": fields}}
    desired_path, current_path, operations_path = (tmp_path / name for name in ("desired.jsonl", "current.jsonl", "operations.jsonl"))
    desired_path.write_bytes(canonical_jsonl([desired]))
    current_path.write_bytes(canonical_jsonl([current], current=True))
    reconcile(desired_path, current_path, operations_path, ScopePolicy("warmhub-data/example", "managed/**", True, "preserve"), reference_fields={"Example": fields})
    assert parse_jsonl(operations_path.read_bytes())[0]["data"][field] == historical


def test_list_revision_spans_batch_boundary_after_new_members(tmp_path):
    from mushroom.submission import iter_operation_batches

    members = [{"name": f"managed/member/{number:04}", "shapes": [], "data": {}} for number in range(1000)]
    desired_list = {"name": "managed/a-list", "shapes": ["List"],
                    "data": {"items": [member["name"] for member in members] + ["managed/prior"]}}
    current_list = {**desired_list, "active": True, "version": 3,
                    "data": {"items": ["managed/prior@v1"]}, "validatedShapes": ["List@v1"],
                    "referenceFields": {"List@v1": {"items": ["wref"]}}}
    desired_path, current_path, operations_path = (tmp_path / name for name in ("desired.jsonl", "current.jsonl", "operations.jsonl"))
    desired_path.write_bytes(canonical_jsonl([*members, desired_list]))
    current_path.write_bytes(canonical_jsonl([current_list], current=True))
    policy = ScopePolicy("warmhub-data/example", "managed/**", True, "preserve")
    reconcile(desired_path, current_path, operations_path, policy)
    batches = iter_operation_batches(parse_jsonl(operations_path.read_bytes()))
    assert [len(batch) for batch in batches] == [1000, 1]
    assert {row["name"] for row in batches[0]} == {member["name"] for member in members}
    assert batches[1][0]["operation"] == "revise"
    assert batches[1][0]["expectedVersion"] == 3
    assert batches[1][0]["data"]["items"][-1] == "managed/prior@v1"
    readback_list = {**current_list, "version": 4, "data": batches[1][0]["data"]}
    current_path.write_bytes(canonical_jsonl([*(dict(member, active=True, version=1) for member in members), readback_list], current=True))
    residual = reconcile(desired_path, current_path, operations_path, policy)
    assert residual.summary.operation_count == 0


def test_reference_binding_filter_walks_nested_and_array_fields():
    from mushroom.references import map_reference_fields, reference_bindings

    fields = {"nested": {"history": [{"type": "wref", "binding": "versioned"}],
                         "owner?": {"type": "wref", "binding": "identity"}},
              "link": {"type": "wref", "binding": "either"}}
    data = {"nested": {"history": ["a", "b@v1"], "owner": "c"}, "link": "d"}
    seen = []

    def remember(reference):
        seen.append(reference)
        return reference

    map_reference_fields(data, fields, remember, binding_filter={"versioned"})
    assert seen == ["a", "b@v1"]
    assert reference_bindings(fields) == {
        ("nested", "history", "[]"): frozenset({"versioned"}),
        ("nested", "owner"): frozenset({"identity"}),
        ("link",): frozenset({"identity", "versioned"}),
    }
