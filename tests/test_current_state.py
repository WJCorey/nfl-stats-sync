from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest

from mushroom.current_state import CurrentStateError, _map_source_references, _matches_scope, canonical_current_state, capture_current_state
from mushroom.kernel import KernelError, ScopePolicy, canonical_jsonl, parse_jsonl, reconcile
from mushroom.references import map_typed_references


def _row(**overrides: object) -> dict[str, object]:
    return {"kind": "thing", "name": "managed/one", "declaredShapes": ["Person@v2"], "validatedShapes": ["Person@v2"], "version": 2, "active": True, "metadata": {"durableId": "durable-1"}, "data": {"label": "One"}} | overrides


def test_canonical_current_state_preserves_complete_multishape_body_and_declarations() -> None:
    row = _row(declaredShapes=["Person@v2", "Agent@v1"], validatedShapes=["Person@v2", "Agent@v1", "Base@v3"], data={"label": "One", "role": "member"})
    count, payload = canonical_current_state([row])
    assert count == 1
    assert parse_jsonl(payload) == [{"name": "managed/one", "shapes": ["Agent", "Person"], "version": 2, "active": True, "durableId": "durable-1", "validatedShapes": ["Agent@v1", "Base@v3", "Person@v2"], "data": {"label": "One", "role": "member"}}]


def test_capture_reads_once_and_uses_pinned_certificate_closure_not_current_shape_head() -> None:
    durable = "040SW8J5EXAMPLE@v1"
    row = _row(declaredShapes=["Observation@v2", "Label@v3"], validatedShapes=["Observation@v2", "Label@v3", "Base@v1"], data={"station": durable, "note": durable, "other": "foreign@v2"})
    schemas = {"Observation@v2": {"note": "string"}, "Label@v3": {"other": "wref"}, "Base@v1": {"station": "wref"}}
    calls: list[dict[str, object]] = []

    def head_iter(**options: object) -> list[SimpleNamespace]:
        calls.append(options)
        return [SimpleNamespace(kind="shape", as_dict=lambda: _row(name="managed/schema")), SimpleNamespace(kind="thing", as_dict=lambda: deepcopy(row)), SimpleNamespace(kind="thing", as_dict=lambda: _row(name="other/one"))]

    def get_many(refs: list[str], **options: object) -> SimpleNamespace:
        assert options == {"include_retracted": True}
        assert refs == ["hub/shapes/Base@v1", "hub/shapes/Label@v3", "hub/shapes/Observation@v2"]
        return SimpleNamespace(missing=[], items=[SimpleNamespace(kind="shape", data={"fields": schemas[ref.removeprefix("hub/shapes/")]}) for ref in refs])

    def head_versions(refs: list[str]) -> SimpleNamespace:
        assert refs == [durable, "foreign@v2"]
        return SimpleNamespace(missing=[], items=[SimpleNamespace(canonical_wref="wh:example/repo/stations/41004"), SimpleNamespace(canonical_wref="wh:other/repo/stations/100")])

    things = SimpleNamespace(head_iter=head_iter, get_many=get_many, head_versions=head_versions)
    client = SimpleNamespace(repository=lambda target: SimpleNamespace(things=things))
    count, payload = capture_current_state(client, target="example/repo", pattern="managed/**")
    assert count == 1
    assert len(calls) == 1
    assert calls[0] == {"data_mode": "full", "include_validated_shapes": True, "exclude_infra_shapes": True, "include_retracted": True, "limit": 500}
    assert parse_jsonl(payload)[0]["data"] == {"station": "stations/41004@v1", "note": durable, "other": "wh:other/repo/stations/100@v2"}
    assert row["data"]["station"] == durable  # Capture never mutates the SDK snapshot.


def test_structural_selectors_and_shape_assertion_subject_survive_resolution() -> None:
    durable = "040SW8J5EXAMPLE@v3"
    row = _row(declaredShapes=["Relation@v1"], validatedShapes=["Relation@v1", "hub/shapes/Arc@v1", "hub/shapes/Assertion@v1"], data={"from": durable, "to": "hub/shapes/Station@v2", "about": durable, "role": "member"})
    fields = {"Relation@v1": {"role": "string", "from": "wref", "about": "wref"}, "hub/shapes/Arc@v1": {"from": "wref", "to": "wref"}, "hub/shapes/Assertion@v1": {"about": "wref"}}
    _map_source_references(row, fields, lambda ref: "managed/station" if ref == durable else ref)
    assert row["data"] == {"from": "managed/station@v3", "to": "hub/shapes/Station@v2", "about": "managed/station@v3", "role": "member"}


def test_typed_reference_walker_handles_optional_null_and_type_named_fields() -> None:
    assert map_typed_references({"station": None}, {"station?": "wref?"}, lambda _: pytest.fail("optional null must not resolve")) == {"station": None}
    assert map_typed_references({"payload": {"type": "event", "station": "durable@v1"}}, {"payload": {"type": "string", "station": "wref"}}, lambda _: "stations/41004") == {"payload": {"type": "event", "station": "stations/41004"}}


def test_local_scope_uses_whole_thing_name() -> None:
    assert _matches_scope(_row(), "managed/**")
    assert not _matches_scope(_row(name="other/one"), "managed/**")
    with pytest.raises(CurrentStateError, match="unpinned Thing name"):
        _matches_scope(_row(name=None), "managed/**")


@pytest.mark.parametrize("rows", [[_row(version=True)], [_row(name="managed/one@v2")], [_row(declaredShapes=None)], [_row(), _row()], [_row(), _row(metadata={"durableId": "other"})], [_row(), _row(name="managed/two")]])
def test_canonical_current_state_rejects_invalid_or_duplicate_sdk_rows(rows: list[dict[str, object]]) -> None:
    with pytest.raises(CurrentStateError):
        canonical_current_state(rows)


@pytest.mark.parametrize("overrides", [{"validatedShapes": None}, {"validatedShapes": ["Person"]}, {"validatedShapes": []}])
def test_capture_fails_closed_without_pinned_complete_certificates(overrides: dict[str, object]) -> None:
    row = _row(**overrides)
    things = SimpleNamespace(head_iter=lambda **_: [SimpleNamespace(kind="thing", as_dict=lambda: deepcopy(row))])
    client = SimpleNamespace(repository=lambda target: SimpleNamespace(things=things))
    with pytest.raises(CurrentStateError):
        capture_current_state(client, target="example/repo", pattern="managed/**")


def test_capture_preserves_builtin_composition_and_shape_subject() -> None:
    row = _row(declaredShapes=["Relation@v1"], validatedShapes=["Relation@v1", "hub/shapes/Arc@v1", "hub/shapes/Assertion@v1"], data={"from": "durable@v4", "to": "hub/shapes/Station@v2", "about": "durable@v4"})
    fields = {"Relation@v1": {"from": "wref", "about": "wref"}, "hub/shapes/Arc@v1": {"from": "wref", "to": "wref"}, "hub/shapes/Assertion@v1": {"about": "wref"}}

    def head_versions(refs: list[str]) -> SimpleNamespace:
        assert refs == ["durable@v4"]
        return SimpleNamespace(missing=[], items=[SimpleNamespace(canonical_wref="wh:example/repo/managed/station")])

    things = SimpleNamespace(head_iter=lambda **_: [SimpleNamespace(kind="thing", as_dict=lambda: deepcopy(row))], get_many=lambda refs, **_options: SimpleNamespace(missing=[], items=[SimpleNamespace(kind="shape", data={"fields": fields[ref if ref in fields else ref.removeprefix("hub/shapes/")]}) for ref in refs]), head_versions=head_versions)
    count, payload = capture_current_state(SimpleNamespace(repository=lambda _: SimpleNamespace(things=things)), target="example/repo", pattern="managed/**")
    assert count == 1
    assert parse_jsonl(payload)[0]["shapes"] == ["Relation"]
    assert parse_jsonl(payload)[0]["data"] == {"from": "managed/station@v4", "to": "hub/shapes/Station@v2", "about": "managed/station@v4"}


@pytest.mark.parametrize("broken_read", ["schema", "reference"])
def test_capture_rejects_incomplete_reads(broken_read: str) -> None:
    row = _row(data={"station": "durable@v1"})
    things = SimpleNamespace(head_iter=lambda **_: [SimpleNamespace(kind="thing", as_dict=lambda: deepcopy(row))], get_many=lambda _, **_options: SimpleNamespace(missing=["Person@v2"] if broken_read == "schema" else [], items=[] if broken_read == "schema" else [SimpleNamespace(kind="shape", data={"fields": {"station": "wref"}})]), head_versions=lambda _: SimpleNamespace(missing=["durable@v1"], items=[]))
    with pytest.raises(CurrentStateError, match="cannot"):
        capture_current_state(SimpleNamespace(repository=lambda _: SimpleNamespace(things=things)), target="example/repo", pattern="managed/**")


@pytest.mark.parametrize("arc_certificate", ["Arc@v1", "hub/shapes/Arc@v1"])
def test_inherited_arc_certificate_survives_canonical_readback_and_converges(tmp_path: Path, arc_certificate: str) -> None:
    row = _row(declaredShapes=["Relation@v1"], validatedShapes=["Relation@v1", arc_certificate],
               referenceFields={"Relation@v1": {}, arc_certificate: {"from": "wref", "to": "wref"}},
               data={"from": "managed/left@v4", "to": "managed/right@v2"})
    _, current = canonical_current_state([row])
    desired = {"name": "managed/one", "shapes": ["Relation"], "data": {"from": "managed/left", "to": "managed/right"}}
    desired_path = tmp_path / "desired.jsonl"
    current_path = tmp_path / "current.jsonl"
    operations_path = tmp_path / "operations.jsonl"
    desired_path.write_bytes(canonical_jsonl([desired]))
    current_path.write_bytes(current)
    result = reconcile(desired_path, current_path, operations_path, ScopePolicy("example/repo", "managed/**", True, "preserve"), reference_fields={"Relation": {"from": "wref", "to": "wref"}})
    assert operations_path.read_bytes() == b""
    assert result.summary.unchanged_count == 1


def test_capture_conforms_to_installed_sdk_response_models_without_network() -> None:
    import httpx
    import json
    from warmhub import WarmHubClient

    durable_reference = "040SW8J5EXAMPLE@v1"
    head_row = _row(data={"station": durable_reference, "note": durable_reference}, declaredShapes=["Person@v2", "wh:other/repo/Label@v3"], validatedShapes=["Person@v2", "wh:other/repo/Label@v3"]) | {
        "wref": "wh:example/repo/managed/one",
        "createdAt": 1_700_000_000_000,
    }
    responses = [
        ("thing.head", {"items": [head_row], "nextCursor": None}),
        ("thing.getMany", {
            "requested": 2,
            "missing": [],
            "items": [{
                "wref": "hub/shapes/Person@v2", "name": "Person", "kind": "shape",
                "declaredShapes": [], "validatedShapes": [], "version": 2,
                "active": True,
                "metadata": {"durableId": "shape-durable", "createdOn": 1_700_000_000_000, "revisedOn": 1_700_000_000_000},
                "data": {"fields": {"station": {"type": "wref", "binding": "versioned"}, "note": "string"}},
            }, {
                "wref": "wh:other/repo/hub/shapes/Label@v3", "name": "Label", "kind": "shape",
                "declaredShapes": [], "validatedShapes": [], "version": 3, "active": True,
                "metadata": {"durableId": "foreign-shape", "createdOn": 1_700_000_000_000, "revisedOn": 1_700_000_000_000},
                "data": {"fields": {"note": "string"}},
            }],
        }),
        ("thing.headVersions", {
            "requested": 1,
            "missing": [],
            "items": [{
                "wref": durable_reference,
                "canonicalWref": "wh:example/repo/stations/41004",
                "durableId": "station-durable", "version": 1, "active": True,
                "revisedOn": 1_700_000_000_000, "dataHash": "server-hash",
            }],
        }),
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        method, payload = responses.pop(0)
        assert request.url.path.endswith(method)
        if method == "thing.getMany":
            body = json.loads(request.content)
            body = body["0"] if "0" in body else body
            assert body["wrefs"] == ["hub/shapes/Person@v2", "wh:other/repo/hub/shapes/Label@v3"]
            assert body["includeRetracted"] is True
        envelope = {"result": {"data": payload}}
        return httpx.Response(200, json=[envelope] if request.url.params.get("batch") == "1" else envelope)

    with httpx.Client(transport=httpx.MockTransport(handler)) as http_client:
        with WarmHubClient(api_url="http://warmhub.test", access_token="synthetic-test-token", http_client=http_client) as client:
            count, payload = capture_current_state(client, target="example/repo", pattern="managed/**")

    assert responses == []
    assert count == 1
    assert parse_jsonl(payload) == [{
        "name": "managed/one", "shapes": ["Person", "wh:other/repo/Label"],
        "validatedShapes": ["Person@v2", "wh:other/repo/Label@v3"], "active": True, "version": 2,
        "durableId": "durable-1",
        "data": {"station": "stations/41004@v1", "note": durable_reference},
        "referenceFields": {"Person@v2": {"station": {"type": "wref", "binding": "versioned"}, "note": "string"}, "wh:other/repo/Label@v3": {"note": "string"}},
    }]


def test_capture_sdk_tombstones_and_new_name_holders_change_current_evidence(tmp_path) -> None:
    import httpx
    import json
    from warmhub import WarmHubClient

    old = _row(active=False, version=2, data={"label": "One", "station": "retracted-endpoint@v1"}) | {"wref": "wh:example/repo/managed/one", "createdAt": 100}
    new = _row(metadata={"durableId": "new-identity"}, version=1) | {"wref": "wh:example/repo/managed/one", "createdAt": 200}
    schema = {
        "wref": "hub/shapes/Person@v2", "kind": "shape", "name": "Person", "version": 2,
        "declaredShapes": [], "validatedShapes": [],
        "active": True, "metadata": {"durableId": "schema-id", "createdOn": 1, "revisedOn": 1},
        "data": {"fields": {"label": "string", "station?": "wref"}},
    }
    snapshots = [[], [old], [old, new], [old, new | {"active": False, "version": 2, "createdAt": 300}]]
    current = []

    def handler(request):
        body = json.loads(request.content) if request.content else json.loads(request.url.params["input"])
        body = body["0"] if "0" in body else body
        if request.url.path.endswith("thing.head"):
            assert body["includeRetracted"] is True
            payload = {"items": current, "nextCursor": None}
        else:
            assert request.url.path.endswith("thing.getMany")
            payload = {"requested": 1, "missing": [], "items": [schema]}
        envelope = {"result": {"data": payload}}
        return httpx.Response(200, json=[envelope] if request.url.params.get("batch") == "1" else envelope)

    evidence = []
    with httpx.Client(transport=httpx.MockTransport(handler)) as http_client:
        with WarmHubClient(api_url="http://warmhub.test", access_token="synthetic", http_client=http_client) as client:
            for current in snapshots:
                evidence.append(capture_current_state(client, target="example/repo", pattern="managed/**")[1])
    assert len(set(evidence)) == 4
    assert evidence[0] == b""
    tombstone = parse_jsonl(evidence[1])[0]
    assert tombstone["active"] is False and tombstone["version"] == 2
    assert tombstone["data"] == {"label": "One", "station": "retracted-endpoint@v1"}
    assert tombstone["shapes"] == ["Person"]
    assert tombstone["validatedShapes"] == ["Person@v2"]
    assert parse_jsonl(evidence[2])[0]["durableId"] == "new-identity"
    assert parse_jsonl(evidence[3])[0]["durableId"] == "new-identity"
    desired_path, current_path, operations_path = (tmp_path / name for name in ("desired.jsonl", "current.jsonl", "operations.jsonl"))
    desired_path.write_bytes(canonical_jsonl([{"name": "managed/one", "shapes": ["DifferentPerson"], "data": {"label": "One"}}]))
    current_path.write_bytes(evidence[3])
    with pytest.raises(KernelError, match="declared Shapes changed"):
        reconcile(desired_path, current_path, operations_path, ScopePolicy("example/repo", "managed/**", True, "preserve"), reference_fields={"DifferentPerson": {"label": "string"}})


def test_capture_rejects_ambiguous_name_holders():
    from mushroom.current_state import _current_name_heads

    with pytest.raises(CurrentStateError, match="ambiguous current name holders"):
        _current_name_heads([_row(), _row(metadata={"durableId": "other"})])
    with pytest.raises(CurrentStateError, match="cannot order retracted name holders"):
        _current_name_heads([_row(active=False, createdAt=1), _row(active=False, createdAt=1)])


@pytest.mark.parametrize("certificate", ["wh:other/repo/Arc@v1", "wh:other/repo/hub/shapes/Arc@v1"])
def test_foreign_arc_shape_preserves_typed_reference_selectors(certificate: str) -> None:
    row = _row(declaredShapes=[certificate], validatedShapes=[certificate], data={"from": "durable@v4", "to": "durable@v2"})
    _map_source_references(row, {certificate: {"from": "wref", "to": "wref"}}, lambda _: "managed/station")
    assert row["data"] == {"from": "managed/station@v4", "to": "managed/station@v2"}


@pytest.mark.parametrize("binding", ["identity", "versioned", "either"])
def test_typed_reference_walker_recognizes_binding_modes_nested_and_arrays(binding: str) -> None:
    data = {"nested": {"station": "durable@v3"}, "stations": ["durable@v2", None]}
    definition = {
        "nested": {"station": {"type": "wref", "shape": "Station", "binding": binding}},
        "stations": {"type": "array", "items": {"type": "wref?", "binding": binding}},
    }
    assert map_typed_references(data, definition, lambda ref: "local/" + ref) == {
        "nested": {"station": "local/durable@v3"}, "stations": ["local/durable@v2", None],
    }


@pytest.mark.parametrize("binding,expected", [("versioned", "stations/a@v1"), ("identity", "stations/a"), ("either", "stations/a")])
def test_typed_reference_walker_keeps_versioned_default_only(binding: str, expected: str) -> None:
    definition = {"nested": {"items": [{"type": "wref", "binding": binding}]}}
    data = {"nested": {"items": ["stations/a", "stations/b@v4", "stations/new"]}}
    current = {"nested": {"items": ["stations/a@v1", "stations/b@v2"]}}
    assert map_typed_references(data, definition, lambda ref: ref, current=current) == {
        "nested": {"items": [expected, "stations/b@v4", "stations/new"]},
    }


def test_typed_reference_walker_handles_objects_with_type_and_binding_fields() -> None:
    schema = {"type": "wref", "binding": "string"}
    assert map_typed_references({"type": "durable@v1", "binding": "literal"}, schema, lambda ref: "station/a@v1") == {
        "type": "station/a@v1", "binding": "literal",
    }


@pytest.mark.parametrize("fields,data,expected", [
    ({"type": "wref"}, {"type": "durable@v1"}, {"type": "managed/y@v1"}),
    ({"payload": {"type": "wref", "binding": "string"}},
     {"payload": {"type": "durable@v1", "binding": "literal"}},
     {"payload": {"type": "managed/y@v1", "binding": "literal"}}),
])
def test_capture_distinguishes_shape_field_root_from_nested_type_specs(fields, data, expected):
    row = _row(data=data)
    _map_source_references(row, {"Person@v2": fields}, lambda ref: "managed/y@v1")
    assert row["data"] == expected


def test_capture_resolves_ordinary_paths_containing_shapes() -> None:
    from mushroom.current_state import _reference_labels

    refs = ["catalog/shapes/x@v1", "wh:other/repo/catalog/shapes/y@v2", "hub/shapes/Station@v1", "wh:other/repo/hub/shapes/Base@v2"]
    requested = []

    def head_versions(references):
        requested.extend(references)
        return SimpleNamespace(missing=[], items=[
            SimpleNamespace(canonical_wref="wh:example/repo/catalog/shapes/renamed"),
            SimpleNamespace(canonical_wref="wh:other/repo/catalog/shapes/renamed"),
        ])

    labels = _reference_labels(SimpleNamespace(head_versions=head_versions), refs, "example/repo")
    assert requested == refs[:2]
    assert labels == {
        refs[0]: "catalog/shapes/renamed@v1", refs[1]: "wh:other/repo/catalog/shapes/renamed@v2",
        refs[2]: refs[2], refs[3]: refs[3],
    }


def test_ordinary_nested_reference_pins_survive_normalization() -> None:
    row = _row(data={"nested": {"station": "durable@v1"}, "stations": ["durable@v2", "durable"]})
    fields = {"Person@v2": {
        "nested": {"station": {"type": "wref", "binding": "versioned"}},
        "stations": {"type": "array", "items": {"type": "wref", "binding": "either"}},
    }}
    _map_source_references(row, fields, lambda _: "stations/a")
    assert row["data"] == {"nested": {"station": "stations/a@v1"}, "stations": ["stations/a@v2", "stations/a"]}


def test_certificate_walk_preserves_shared_nested_parent_fields_and_checks_closure() -> None:
    row = _row(validatedShapes=["Person@v2", "Location@v1"], data={"nested": {"station": "durable@v2", "subject": "other@v3"}})
    fields = {
        "Person@v2": {"nested": {"station": "wref"}},
        "Location@v1": {"nested": {"subject": {"type": "wref", "binding": "either"}}},
    }
    _map_source_references(row, fields, lambda ref: {"durable@v2": "stations/a", "other@v3": "subjects/b"}.get(ref, ref))
    assert row["data"] == {"nested": {"station": "stations/a@v2", "subject": "subjects/b@v3"}}
    with pytest.raises(CurrentStateError, match="unread certified Shape"):
        _map_source_references(row, {"Person@v2": fields["Person@v2"]}, lambda ref: ref)


def test_overlapping_certificates_normalize_from_original_reference_only_once() -> None:
    row = _row(validatedShapes=["Person@v2", "Base@v1"], data={"nested": {"station": "durable@v2"}})
    fields = {certificate: {"nested": {"station": "wref"}} for certificate in row["validatedShapes"]}
    _map_source_references(row, fields, lambda ref: {"durable@v2": "managed/first", "managed/first@v2": "managed/wrong"}.get(ref, ref))
    assert row["data"] == {"nested": {"station": "managed/first@v2"}}


@pytest.mark.parametrize("active", [True, False])
@pytest.mark.parametrize("native", [None, "managed/subject@v1"])
def test_indexed_assertion_subject_normalizes_without_inventing_declarations(active, native) -> None:
    row = _row(declaredShapes=["SemanticContract@v1"], validatedShapes=["SemanticContract@v1"], aboutWref="durable@v1", active=active, data={"definition": "Reviewed"})
    if native is not None:
        row["data"]["about"] = native
    fields = {"SemanticContract@v1": {"definition": "string"}}
    _map_source_references(row, fields, lambda ref: "managed/subject@v1" if ref == "durable@v1" else ref)
    count, payload = canonical_current_state([row])
    assert count == 1
    record = parse_jsonl(payload)[0]
    assert record["data"]["about"] == record["assertionSubject"] == "managed/subject@v1"
    assert record["shapes"] == ["SemanticContract"]
    assert record["validatedShapes"] == ["SemanticContract@v1"]
    assert record["version"] == 2
    assert record["active"] is active
    assert record.get("needsAssertionAdoption") is (True if native is None else None)


@pytest.mark.parametrize("native", ["managed/other@v1", "managed/subject@v2", None])
def test_indexed_assertion_refuses_conflicting_native_subject(native) -> None:
    row = _row(aboutWref="managed/subject@v1", data={"about": native})
    with pytest.raises(CurrentStateError, match="conflicts"):
        canonical_current_state([row])


@pytest.mark.parametrize("has_assertion_certificate", [True, False])
def test_legacy_assertion_sdk_capture_adopts_once_with_exact_historical_subject(tmp_path, has_assertion_certificate) -> None:
    import httpx
    import json
    from warmhub import WarmHubClient

    certificates = ["SemanticContract@v1"] + (["Assertion@v1"] if has_assertion_certificate else [])
    definitions = [("Assertion", {})] if has_assertion_certificate else []
    definitions.append(("SemanticContract", {"definition": "string"}))
    row = _row(declaredShapes=["SemanticContract@v1"], validatedShapes=certificates,
               aboutWref="hub/shapes/Person@v1", data={"definition": "Reviewed"},
               wref="wh:example/repo/managed/one", createdAt=1_700_000_000_000)
    methods = []

    def handler(request):
        method = request.url.path.rsplit("/", 1)[-1]
        methods.append(method)
        body = json.loads(request.content)
        body = body["0"] if "0" in body else body
        if method == "thing.head":
            assert body == {"orgName": "example", "repoName": "repo", "dataMode": "full", "includeValidatedShapes": True,
                            "excludeInfraShapes": True, "includeRetracted": True, "limit": 500}
            payload = {"items": [row], "nextCursor": None}
        else:
            assert method == "thing.getMany"
            assert body["wrefs"] == [f"hub/shapes/{shape}@v1" for shape, _ in definitions]
            assert body["includeRetracted"] is True
            payload = {"requested": len(definitions), "missing": [], "items": [
                {"wref": f"hub/shapes/{shape}@v1", "name": shape, "kind": "shape", "version": 1, "active": True,
                 "declaredShapes": [], "validatedShapes": [], "metadata": {"durableId": shape, "createdOn": 1_700_000_000_000, "revisedOn": 1_700_000_000_000},
                 "data": {"fields": fields}}
                for shape, fields in definitions
            ]}
        envelope = {"result": {"data": payload}}
        return httpx.Response(200, json=[envelope] if request.url.params.get("batch") == "1" else envelope)

    with httpx.Client(transport=httpx.MockTransport(handler)) as transport:
        with WarmHubClient(api_url="http://warmhub.test", access_token="synthetic-test-token", http_client=transport) as client:
            _, current = capture_current_state(client, target="example/repo", pattern="managed/**")
    assert methods == ["thing.head", "thing.getMany"]
    desired = {"name": "managed/one", "shapes": ["SemanticContract"],
               "data": {"definition": "Reviewed", "about": "hub/shapes/Person@v1"}}
    desired_path, current_path, operations_path = [tmp_path / name for name in ("desired.jsonl", "current.jsonl", "ops.jsonl")]
    current_path.write_bytes(current)
    desired_path.write_bytes(canonical_jsonl([desired]))
    policy = ScopePolicy("example/repo", "managed/**", True, "preserve")
    contracts = {"SemanticContract": {"definition": "string", "about": "wref"}}
    result = reconcile(desired_path, current_path, operations_path, policy, reference_fields=contracts)
    assert result.summary.revise_count == 1
    assert parse_jsonl(operations_path.read_bytes()) == [{"operation": "revise", "expectedVersion": 2, **desired}]
    row["data"]["about"] = row["aboutWref"]
    row["version"] = 3
    methods.clear()
    with httpx.Client(transport=httpx.MockTransport(handler)) as transport:
        with WarmHubClient(api_url="http://warmhub.test", access_token="synthetic-test-token", http_client=transport) as client:
            _, native_current = capture_current_state(client, target="example/repo", pattern="managed/**")
    assert "needsAssertionAdoption" not in parse_jsonl(native_current)[0]
    current_path.write_bytes(native_current)
    result = reconcile(desired_path, current_path, operations_path, policy, reference_fields=contracts)
    assert result.summary.unchanged_count == 1
    assert operations_path.read_bytes() == b""
    desired["data"]["definition"] = "Updated after review"
    desired_path.write_bytes(canonical_jsonl([desired]))
    reconcile(desired_path, current_path, operations_path, policy, reference_fields=contracts)
    assert parse_jsonl(operations_path.read_bytes()) == [{"operation": "revise", "expectedVersion": 3, **desired}]


def test_capture_compares_indexed_and_native_subjects_after_identity_resolution() -> None:
    row = _row(aboutWref="durable@v1", data={"about": "managed/subject@v1"})
    requests = []

    def head_versions(refs):
        requests.append(refs)
        return SimpleNamespace(missing=[], items=[SimpleNamespace(canonical_wref="wh:example/repo/managed/subject") for _ in refs])

    things = SimpleNamespace(
        head_iter=lambda **_: [SimpleNamespace(kind="thing", as_dict=lambda: deepcopy(row))],
        get_many=lambda *args, **kwargs: SimpleNamespace(missing=[], items=[SimpleNamespace(kind="shape", data={"fields": {}})]),
        head_versions=head_versions,
    )
    _, payload = capture_current_state(SimpleNamespace(repository=lambda _: SimpleNamespace(things=things)), target="example/repo", pattern="managed/**")
    assert requests == [["durable@v1", "managed/subject@v1"]]
    assert parse_jsonl(payload)[0]["assertionSubject"] == "managed/subject@v1"
