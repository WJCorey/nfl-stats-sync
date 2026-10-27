"""Shared traversal of schema-typed references; ordinary strings remain data."""

from collections.abc import Callable, Mapping
import re

from warmhub.rules.shape_validation import _is_type_spec_object

BUILTIN_REFERENCE_FIELDS = {
    "Content": {},
    "Arc": {"from": "wref", "to": "wref"},
    "Bond": {"ends": ["wref"]},
    "List": {"items": ["wref"]},
    "Pair": {"first": "wref", "second": "wref"},
    "Set": {"members": ["wref"]},
    "Triple": {"first": "wref", "second": "wref", "third": "wref"},
    "Assertion": {"about": "wref"},
}

def shape_name(reference: str) -> str:
    unpinned = re.sub(r"@v[1-9][0-9]*$", "", reference)
    if unpinned.startswith("wh:"):
        return unpinned
    if "/shapes/" in unpinned:
        return unpinned.rsplit("/shapes/", 1)[1]
    return unpinned.removeprefix("shapes/")


def _array_history(value: list, item_type: object, current: object) -> list:
    history = current if isinstance(current, list) else []
    declared = item_type["type"] if _is_type_spec_object(item_type) else item_type
    binding = item_type.get("binding", "versioned") if _is_type_spec_object(item_type) else "versioned"
    if not isinstance(declared, str) or declared.removesuffix("?") != "wref" or binding != "versioned":
        # shortcut: object arrays pair by position until an item identity contract exists.
        return history
    by_identity: dict[str, set[str]] = {}
    for reference in history:
        if isinstance(reference, str):
            by_identity.setdefault(re.sub(r"@v[1-9][0-9]*$", "", reference), set()).add(reference)
    matched = []
    for reference in value:
        candidates = by_identity.get(reference, set()) if isinstance(reference, str) and "@" not in reference else set()
        if len(candidates) > 1:
            raise ValueError("ambiguous historical array reference; specify an explicit version")
        matched.append(next(iter(candidates)) if candidates else None)
    return matched


def map_reference_fields(
    value: object,
    fields: Mapping[str, object],
    replace: Callable[[str], str],
    *,
    current: object = None,
    binding_filter: set[str] | None = None,
) -> object:
    if not isinstance(value, dict):
        raise ValueError("SDK object field is not an object")
    for field, nested_type in fields.items():
        name = field.removesuffix("?")
        if name in value:
            value[name] = map_typed_references(
                value[name], nested_type, replace, optional=field.endswith("?"),
                current=current.get(name) if isinstance(current, Mapping) else None,
                binding_filter=binding_filter,
            )
    return value


def map_typed_references(
    value: object,
    type_definition: object,
    replace: Callable[[str], str],
    *,
    optional: bool = False,
    current: object = None,
    binding_filter: set[str] | None = None,
) -> object:
    type_spec = _is_type_spec_object(type_definition)
    declared = type_definition["type"] if type_spec else type_definition
    base_type = declared.removesuffix("?") if isinstance(declared, str) else None
    if base_type == "wref":
        if value is None and (optional or declared.endswith("?")):
            return value
        if not isinstance(value, str):
            raise ValueError("SDK wref field is not a string")
        binding = type_definition.get("binding", "versioned") if type_spec else "versioned"
        if binding not in {"identity", "versioned", "either"}:
            raise ValueError("SDK wref field has an invalid binding")
        if (
            binding == "versioned" and isinstance(current, str)
            and "@" not in value and re.sub(r"@v[1-9][0-9]*$", "", current) == value
        ):
            value = current
        return replace(value) if binding_filter is None or binding in binding_filter else value
    if value is None:
        return value
    if isinstance(type_definition, list) or type_spec and base_type == "array":
        if not isinstance(value, list):
            raise ValueError("SDK array field is not a list")
        if isinstance(type_definition, list):
            item_type = type_definition[0] if type_definition else None
        else:
            item_type = type_definition.get("items")
        history = _array_history(value, item_type, current)
        for index, item in enumerate(value):
            value[index] = map_typed_references(
                item, item_type, replace,
                current=history[index] if index < len(history) else None,
                binding_filter=binding_filter,
            )
        return value
    if isinstance(type_definition, Mapping) and not type_spec:
        return map_reference_fields(value, type_definition, replace, current=current, binding_filter=binding_filter)
    return value



def reference_bindings(fields: Mapping[str, object]) -> dict[tuple[str, ...], frozenset[str]]:
    """Compare effective bindings by schema path, independent of optional syntax."""
    bindings: dict[tuple[str, ...], frozenset[str]] = {}

    def walk(spec: object, path: tuple[str, ...]) -> None:
        typed = _is_type_spec_object(spec)
        declared = spec["type"] if typed else spec
        base = declared.removesuffix("?") if isinstance(declared, str) else None
        if base == "wref":
            binding = spec.get("binding", "versioned") if typed else "versioned"
            modes = frozenset({"identity", "versioned"} if binding == "either" else {binding})
            bindings[path] = bindings.get(path, modes) & modes
        elif isinstance(spec, list):
            if spec:
                walk(spec[0], (*path, "[]"))
        elif typed and base == "array":
            walk(spec.get("items"), (*path, "[]"))
        elif isinstance(spec, Mapping) and not typed:
            for field, child in spec.items():
                walk(child, (*path, field.removesuffix("?")))

    for field, spec in fields.items():
        walk(spec, (field.removesuffix("?"),))
    return bindings
