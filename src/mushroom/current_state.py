"""Optional complete SDK HEAD capture; Dagster keeps using the local fixture by default."""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Mapping
from typing import Protocol

from mushroom.kernel import KernelError, canonical_jsonl, matches_scope


_TARGET = re.compile(r"^[a-z0-9][a-z0-9.-]*/[a-z0-9][a-z0-9.-]*$")
_COLLECTION_FIELDS = {
    "Arc": ("from", "to"),
    "Bond": ("ends",),
    "List": ("items",),
    "Pair": ("first", "second"),
    "Set": ("members",),
    "Triple": ("first", "second", "third"),
}
_TYPE_SPEC_KEYS = {
    "array": {"type", "description", "items", "minItems", "maxItems"},
    "boolean": {"type", "description"},
    "number": {"type", "description", "minimum", "maximum", "integer"},
    "string": {"type", "description", "minLength", "maxLength", "pattern", "enum"},
    "wref": {"type", "description", "shape"},
}


class CurrentStateError(KernelError):
    """The SDK response cannot prove one canonical current state."""


class _Client(Protocol):
    shape: object

    def repository(self, target: str) -> object: ...


def capture_current_state(client: _Client, *, target: str, pattern: str) -> tuple[int, bytes]:
    """Read complete managed SDK HEAD scopes into planner-ready current-state JSONL."""

    if not isinstance(target, str) or _TARGET.fullmatch(target) is None:
        raise CurrentStateError("SDK HEAD target must be an unpinned organization/repository")
    if not isinstance(pattern, str) or not pattern.strip() or "/" not in pattern or "@" in pattern:
        raise CurrentStateError("managed scope must match unpinned Shape/name wrefs")
    org_name, repo_name = target.split("/", 1)
    things = client.repository(target).things  # type: ignore[attr-defined]
    sources = [
        dict(source)
        for kind in ("thing", "collection", "assertion")
        for item in things.head_iter(
            kind=kind,
            data_mode="full",
            exclude_infra_shapes=True,
            include_retracted=False,
            limit=500,
        )
        for source in (item.as_dict(),)
        # The server's kind=thing drain may also return collections (a
        # collection is a thing), which would surface the same durable
        # identity twice across drains. Keep each row only in the drain
        # whose requested kind matches its reported kind.
        if source.get("kind") == kind and _matches_scope(source, pattern)
    ]
    if not sources:
        return canonical_current_state(())
    shape_fields = _shape_fields(client, org_name, repo_name)
    references: set[str] = set()

    def remember(reference: str) -> str:
        references.add(reference)
        return reference

    for source in sources:
        _map_source_references(source, shape_fields, remember)
    labels = _reference_labels(things, sorted(references), target)
    for source in sources:
        _map_source_references(source, shape_fields, labels.__getitem__)
    return canonical_current_state(sources)


def _shape_fields(client: _Client, org_name: str, repo_name: str) -> dict[str, Mapping[str, object]]:
    result: dict[str, Mapping[str, object]] = {}
    shapes = client.shape.list(org_name, repo_name).items  # type: ignore[attr-defined]
    for shape in shapes:
        if not shape.active or shape.version is None:
            continue
        data = shape.version.data
        fields = data.get("fields") if isinstance(data, Mapping) else None
        if not isinstance(fields, Mapping):
            raise CurrentStateError(f"Shape {shape.name} lacks field definitions")
        result[shape.name] = fields
    return result


def _reference_labels(things: object, references: list[str], target: str) -> dict[str, str]:
    if not references:
        return {}
    resolved = things.head_versions(references)  # type: ignore[attr-defined]
    if resolved.missing:
        raise CurrentStateError(f"cannot resolve current reference: {resolved.missing[0]}")
    if len(resolved.items) != len(references):
        raise CurrentStateError("reference resolution returned an incomplete result")
    labels: dict[str, str] = {}
    for reference, item in zip(references, resolved.items, strict=True):
        canonical = item.extra.get("canonicalWref")
        # Desired references are unpinned; the storage pin must not force a revision.
        labels[reference] = (
            item.wref
            if not isinstance(canonical, str) or canonical.startswith(f"wh:{target}/")
            else canonical
        )
    return labels


def _map_source_references(
    source: dict[str, object],
    shape_fields: Mapping[str, Mapping[str, object]],
    replace: Callable[[str], str],
) -> None:
    kind, shape, data = (source.get(key) for key in ("kind", "shapeName", "data"))
    if not isinstance(shape, str) or not isinstance(data, Mapping):
        raise CurrentStateError("SDK row lacks Shape-aware data")

    def replace_structural(reference: str) -> str:
        selector = re.search(r"@v[1-9][0-9]*$", reference)
        resolved = replace(reference)
        return (
            resolved
            if selector is None or resolved.endswith(selector.group())
            else f"{resolved}{selector.group()}"
        )

    if shape in _COLLECTION_FIELDS:
        if not isinstance(data, dict):
            raise CurrentStateError("SDK collection data is not an object")
        for field in _COLLECTION_FIELDS[shape]:
            value = data.get(field)
            if isinstance(value, str):
                data[field] = replace_structural(value)
            elif isinstance(value, list) and all(isinstance(item, str) for item in value):
                for index, item in enumerate(value):
                    value[index] = replace_structural(item)
            else:
                raise CurrentStateError(f"SDK collection field {shape}.{field} is invalid")
    else:
        fields = shape_fields.get(shape)
        if fields is None:
            raise CurrentStateError(f"SDK row references unknown Shape {shape}")
        source["data"] = _map_typed_references(data, fields, replace)
    if kind == "assertion":
        about = source.get("aboutWref")
        if not isinstance(about, str):
            raise CurrentStateError("SDK assertion row lacks an about reference")
        source["aboutWref"] = replace_structural(about)


def _is_type_spec_object(value: object) -> bool:
    if not isinstance(value, Mapping):
        return False
    declared = value.get("type")
    if not isinstance(declared, str):
        return False
    base_type = declared.removesuffix("?")
    return base_type in _TYPE_SPEC_KEYS and set(value) <= _TYPE_SPEC_KEYS[base_type]


def _map_typed_references(
    value: object,
    type_definition: object,
    replace: Callable[[str], str],
    *,
    optional: bool = False,
) -> object:
    type_spec = _is_type_spec_object(type_definition)
    declared = type_definition["type"] if type_spec else type_definition
    base_type = declared.removesuffix("?") if isinstance(declared, str) else None
    if base_type == "wref":
        if value is None and (optional or declared.endswith("?")):
            return value
        if not isinstance(value, str):
            raise CurrentStateError("SDK wref field is not a string")
        return replace(value)
    if value is None:
        return value
    if isinstance(type_definition, list):
        if not isinstance(value, list):
            raise CurrentStateError("SDK array field is not a list")
        item_type = type_definition[0] if type_definition else None
        for index, item in enumerate(value):
            value[index] = _map_typed_references(item, item_type, replace)
        return value
    if type_spec and base_type == "array":
        if not isinstance(value, list):
            raise CurrentStateError("SDK array field is not a list")
        for index, item in enumerate(value):
            value[index] = _map_typed_references(
                item, type_definition.get("items"), replace
            )
        return value
    if isinstance(type_definition, Mapping) and not type_spec:
        if not isinstance(value, dict):
            raise CurrentStateError("SDK object field is not an object")
        for field, nested_type in type_definition.items():
            name = field.removesuffix("?")
            if name in value:
                value[name] = _map_typed_references(
                    value[name], nested_type, replace, optional=field.endswith("?")
                )
    return value


def canonical_current_state(rows: Iterable[Mapping[str, object]]) -> tuple[int, bytes]:
    """Normalize complete SDK HEAD rows into planner-ready current-state JSONL."""

    by_durable_id: dict[str, dict[str, object]] = {}
    durable_id_by_wref: dict[str, str] = {}
    for source in rows:
        durable_id = _durable_id(source)
        if not isinstance(durable_id, str) or not durable_id:
            raise CurrentStateError("SDK row lacks a durable identity")
        if durable_id in by_durable_id:
            raise CurrentStateError("SDK HEAD drains returned duplicate current identities")
        row = _normalize(source)
        wref = row["wref"]
        shape = row["shape"]
        if not isinstance(wref, str) or not isinstance(shape, str) or "@" in wref or wref.partition("/")[0] != shape:
            raise CurrentStateError("SDK row wref must be an unpinned name for its Shape")
        if wref in durable_id_by_wref:
            raise CurrentStateError("SDK HEAD drains returned duplicate current wrefs")
        durable_id_by_wref[wref] = durable_id
        by_durable_id[durable_id] = row
    records = list(by_durable_id.values())
    try:
        return len(records), canonical_jsonl(records, current=True)
    except KernelError as error:
        raise CurrentStateError(str(error)) from None


def _durable_id(source: Mapping[str, object]) -> object:
    value = source.get("durableId")
    metadata = source.get("metadata")
    return value if value is not None else metadata.get("durableId") if isinstance(metadata, dict) else None


def _matches_scope(source: Mapping[str, object], pattern: str) -> bool:
    wref = source.get("wref")
    if not isinstance(wref, str):
        raise CurrentStateError("SDK row lacks an unpinned wref")
    return matches_scope(wref, pattern)


def _normalize(source: Mapping[str, object]) -> dict[str, object]:
    kind, shape, wref, version, active = (source.get(key) for key in ("kind", "shapeName", "wref", "version", "active"))
    if not all(isinstance(value, str) and value for value in (kind, shape, wref)) or isinstance(version, bool) or not isinstance(version, int) or version < 1 or type(active) is not bool:
        raise CurrentStateError("SDK row is not a valid current-state record")
    row: dict[str, object] = {"kind": kind, "shape": shape, "wref": wref, "version": version, "active": active}
    data = source.get("data")
    if kind in {"thing", "collection"} and shape in _COLLECTION_FIELDS:
        fields = _COLLECTION_FIELDS[shape]
        if not isinstance(data, dict):
            raise CurrentStateError("SDK collection data is invalid")
        members = data.get(fields[0]) if len(fields) == 1 else [data.get(field) for field in fields]
        if not isinstance(members, list) or not all(isinstance(member, str) for member in members):
            raise CurrentStateError("SDK collection members are invalid")
        row.update(kind="collection", collectionType=shape.lower(), members=members)
    elif kind == "assertion":
        if not isinstance(data, dict) or not isinstance(source.get("aboutWref"), str):
            raise CurrentStateError("SDK assertion row is invalid")
        row.update(data=data, about=source["aboutWref"])
    elif kind == "thing" and isinstance(data, dict):
        row["data"] = data
    else:
        raise CurrentStateError("SDK row kind is invalid")
    return row
