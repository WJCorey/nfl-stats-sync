from __future__ import annotations

import inspect
from pathlib import Path
from types import SimpleNamespace

import pytest

from mushroom.current_state import (
    CurrentStateError,
    _map_source_references,
    _map_typed_references,
    _matches_scope,
    canonical_current_state,
    capture_current_state,
)
from mushroom.kernel import ScopePolicy, canonical_jsonl, parse_jsonl, reconcile


def _row(*, durable_id: str = "durable-1") -> dict[str, object]:
    return {
        "kind": "thing",
        "shapeName": "Person",
        "wref": "Person/managed/one",
        "version": 2,
        "active": True,
        "durableId": durable_id,
        "data": {"label": "One"},
    }


def test_canonical_current_state_normalizes_complete_sdk_rows() -> None:
    count, payload = canonical_current_state(
        [_row(), _row(durable_id="durable-2") | {"wref": "Person/managed/two"}]
    )

    assert count == 2
    assert [row["wref"] for row in parse_jsonl(payload)] == [
        "Person/managed/one",
        "Person/managed/two",
    ]


def test_canonical_current_state_normalizes_sdk_collection_rows() -> None:
    count, payload = canonical_current_state(
        [
            {
                "kind": "collection",
                "shapeName": "Arc",
                "wref": "Arc/example/alice-science",
                "version": 1,
                "active": True,
                "metadata": {"durableId": "arc-durable-id"},
                "data": {
                    "from": "Person/example/alice@v1",
                    "to": "Committee/example/science@v1",
                },
            }
        ]
    )

    assert count == 1
    assert parse_jsonl(payload) == [
        {
            "active": True,
            "collectionType": "arc",
            "kind": "collection",
            "members": ["Person/example/alice@v1", "Committee/example/science@v1"],
            "shape": "Arc",
            "version": 1,
            "wref": "Arc/example/alice-science",
        }
    ]


def test_live_current_state_drains_each_supported_kind() -> None:
    source = inspect.getsource(capture_current_state)
    assert 'for kind in ("thing", "collection", "assertion")' in source
    assert "match=pattern" not in source
    assert "_matches_scope(source, pattern)" in source


def test_live_current_state_compares_wref_fields_by_resolved_name(tmp_path: Path) -> None:
    durable = "040SW8J5EXAMPLE@v1"
    row = _row() | {
        "shapeName": "Observation",
        "wref": "Observation/managed/one",
        "data": {"station": durable, "note": durable},
    }
    shape = SimpleNamespace(
        active=True,
        name="Observation",
        version=SimpleNamespace(data={"fields": {"station": "wref", "note": "string"}}),
    )

    def head_versions(references: list[str]) -> SimpleNamespace:
        assert references == [durable]
        return SimpleNamespace(
            items=[
                SimpleNamespace(
                    wref="Station/41004",
                    extra={"canonicalWref": "wh:example/repo/Station/41004"},
                )
            ],
            missing=[],
        )

    def list_shapes(org: str, repo: str) -> SimpleNamespace:
        assert (org, repo) == ("example", "repo")
        return SimpleNamespace(items=[shape])

    things = SimpleNamespace(
        head_iter=lambda **options: [SimpleNamespace(as_dict=lambda: row)]
        if options["kind"] == "thing"
        else [],
        head_versions=head_versions,
    )
    client = SimpleNamespace(
        shape=SimpleNamespace(list=list_shapes),
        repository=lambda target: SimpleNamespace(things=things),
    )

    _, payload = capture_current_state(
        client, target="example/repo", pattern="*/managed/**"
    )

    assert parse_jsonl(payload)[0]["data"] == {
        "station": "Station/41004",
        "note": durable,
    }
    desired = row | {
        "kind": "thing",
        "shape": "Observation",
        "data": {"station": "Station/41004", "note": durable},
    }
    for key in ("shapeName", "version", "active", "durableId"):
        desired.pop(key)
    desired_path = tmp_path / "desired.jsonl"
    current_path = tmp_path / "current.jsonl"
    operations_path = tmp_path / "operations.jsonl"
    desired_path.write_bytes(canonical_jsonl([desired]))
    current_path.write_bytes(payload)

    result = reconcile(
        desired_path,
        current_path,
        operations_path,
        ScopePolicy("example/repo", "*/managed/**", True, "preserve"),
    )

    assert operations_path.read_bytes() == b""
    assert result.summary.unchanged_count == 1


def test_structural_reference_selectors_survive_name_resolution() -> None:
    durable = "040SW8J5EXAMPLE@v3"
    collection = {
        "kind": "collection",
        "shapeName": "Set",
        "data": {"members": [durable]},
    }
    assertion = {
        "kind": "assertion",
        "shapeName": "Assignment",
        "aboutWref": durable,
        "data": {"role": "member"},
    }
    references: set[str] = set()

    def remember(reference: str) -> str:
        references.add(reference)
        return reference

    _map_source_references(collection, {}, remember)
    _map_source_references(assertion, {"Assignment": {"role": "string"}}, remember)
    assert references == {durable}

    _map_source_references(collection, {}, lambda _: "Facility/managed/example")
    _map_source_references(
        assertion,
        {"Assignment": {"role": "string"}},
        lambda _: "Facility/managed/example",
    )

    assert collection["data"] == {"members": ["Facility/managed/example@v3"]}
    assert assertion["aboutWref"] == "Facility/managed/example@v3"


def test_typed_reference_walker_handles_optional_null_and_type_named_fields() -> None:
    optional = {"station": None}
    nested = {"payload": {"type": "event", "station": "durable@v1"}}

    assert _map_typed_references(
        optional,
        {"station?": "wref?"},
        lambda _: pytest.fail("null optional reference must not be resolved"),
    ) == {"station": None}
    assert _map_typed_references(
        nested,
        {"payload": {"type": "string", "station": "wref"}},
        lambda _: "Station/41004",
    ) == {"payload": {"type": "event", "station": "Station/41004"}}


def test_local_scope_filter_keeps_only_matching_wrefs() -> None:
    assert _matches_scope(_row(), "*/managed/**")
    assert not _matches_scope(_row() | {"wref": "Person/other/one"}, "*/managed/**")
    assert not _matches_scope(_row() | {"wref": "Person/other/managed/one"}, "*/managed/**")
    with pytest.raises(CurrentStateError, match="lacks an unpinned wref"):
        _matches_scope(_row() | {"wref": None}, "*/managed/**")


@pytest.mark.parametrize(
    "rows",
    [
        [_row() | {"version": True}],
        [_row() | {"wref": "Person/managed/one@v2"}],
        [_row() | {"wref": "Other/managed/one"}],
        [_row(), _row()],
        [_row(durable_id="first"), _row(durable_id="second")],
    ],
)
def test_canonical_current_state_rejects_invalid_sdk_rows(
    rows: list[dict[str, object]],
) -> None:
    with pytest.raises(CurrentStateError):
        canonical_current_state(rows)


def test_live_current_state_deduplicates_collections_returned_by_the_thing_drain() -> None:
    # Observed on prod 2026-09-10: the server's kind=thing HEAD drain also
    # returns collections (a collection is a thing), so the same durable
    # identity arrived from both the thing and collection drains and tripped
    # the duplicate-identity guard. Each drain must keep only rows whose
    # reported kind matches the requested kind.
    collection_row = {
        "kind": "collection",
        "shapeName": "Set",
        "wref": "Set/managed/release",
        "version": 1,
        "active": True,
        "metadata": {"durableId": "set-durable-id"},
        "data": {"members": []},
    }

    def head_iter(**options: object) -> list[SimpleNamespace]:
        if options["kind"] in ("thing", "collection"):
            return [SimpleNamespace(as_dict=lambda: dict(collection_row))]
        return []

    things = SimpleNamespace(head_iter=head_iter)
    client = SimpleNamespace(
        shape=SimpleNamespace(
            list=lambda org, repo: SimpleNamespace(items=[])
        ),
        repository=lambda target: SimpleNamespace(things=things),
    )

    count, payload = capture_current_state(
        client, target="example/repo", pattern="*/managed/**"
    )

    assert count == 1
    assert [row["wref"] for row in parse_jsonl(payload)] == ["Set/managed/release"]


def test_live_current_state_excludes_assertions_about_a_shape() -> None:
    # Ontology-governance assertions (SemanticContract/NamingContract) are
    # about a Shape; their about reference resolves to a single segment,
    # which the reconciler's Shape/name grammar rejects. They are excluded
    # from capture — a data project can never manage them.
    def assertion_row(wref: str, about: str) -> dict[str, object]:
        return {
            "kind": "assertion",
            "shapeName": wref.partition("/")[0],
            "wref": wref,
            "version": 1,
            "active": True,
            "durableId": f"durable-{wref}",
            "data": {"definition": "x"},
            "aboutWref": about,
        }

    rows = [
        assertion_row("SemanticContract/managed/player", "Player@v2"),
        assertion_row("Claim/managed/one", "Person/managed/one"),
    ]

    things = SimpleNamespace(
        head_iter=lambda **options: [
            SimpleNamespace(as_dict=lambda row=row: dict(row)) for row in rows
        ]
        if options["kind"] == "assertion"
        else [],
        head_versions=lambda references: SimpleNamespace(
            items=[
                SimpleNamespace(
                    wref=reference.partition("@")[0],
                    extra={
                        "canonicalWref": "wh:example/repo/"
                        + reference.partition("@")[0]
                    },
                )
                for reference in references
            ],
            missing=[],
        ),
    )
    shapes = [
        SimpleNamespace(
            active=True,
            name=name,
            version=SimpleNamespace(data={"fields": {"definition": "string"}}),
        )
        for name in ("SemanticContract", "Claim")
    ]
    client = SimpleNamespace(
        shape=SimpleNamespace(list=lambda org, repo: SimpleNamespace(items=shapes)),
        repository=lambda target: SimpleNamespace(things=things),
    )

    count, payload = capture_current_state(
        client, target="example/repo", pattern="*/managed/**"
    )

    assert count == 1
    assert [row["wref"] for row in parse_jsonl(payload)] == ["Claim/managed/one"]
