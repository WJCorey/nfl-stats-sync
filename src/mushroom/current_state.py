"""Complete SDK HEAD capture for whole-Thing reconciliation."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from copy import deepcopy
import re
from typing import Protocol

from mushroom.kernel import KernelError, canonical_jsonl, matches_scope
from mushroom.references import map_reference_fields

_TARGET = re.compile(r"^[a-z0-9][a-z0-9.-]*/[a-z0-9][a-z0-9.-]*$")
_PIN = re.compile(r"@v[1-9][0-9]*$")


class CurrentStateError(KernelError):
    """The SDK response cannot prove one canonical current state."""


class _Client(Protocol):
    def repository(self, target: str) -> object: ...


def capture_current_state(client: _Client, *, target: str, pattern: str) -> tuple[int, bytes]:
    """Read each managed Thing once, retaining its complete body and declarations."""
    if not isinstance(target, str) or _TARGET.fullmatch(target) is None:
        raise CurrentStateError("SDK HEAD target must be an unpinned organization/repository")
    if not isinstance(pattern, str) or not pattern.strip() or "@" in pattern:
        raise CurrentStateError("managed scope must match unpinned Thing names")
    things = client.repository(target).things  # type: ignore[attr-defined]
    sources = []
    for item in things.head_iter(
        data_mode="full", include_validated_shapes=True,
        exclude_infra_shapes=True, include_retracted=True, limit=500,
    ):
        if item.kind == "shape":
            continue
        source = item.as_dict()
        if _matches_scope(source, pattern):
            sources.append(source)
    if not sources:
        return canonical_current_state(())
    sources = _current_name_heads(sources)
    certificates: set[str] = set()
    for source in sources:
        declared = source.get("declaredShapes")
        validated = source.get("validatedShapes")
        if not isinstance(declared, list) or not isinstance(validated, list):
            raise CurrentStateError("SDK row lacks declarations or complete Shape certificates")
        if not all(isinstance(ref, str) and _PIN.search(ref) for ref in declared + validated):
            raise CurrentStateError("SDK Shape certificates must be version pinned")
        if not set(declared) <= set(validated):
            raise CurrentStateError("SDK certificate closure omits declared Shapes")
        certificates.update(validated)
    fields = _certified_fields(things, sorted(certificates))
    references: set[str] = set()

    def remember(reference: str) -> str:
        references.add(reference)
        return reference

    for source in sources:
        if source.get("active") is True:
            _map_source_references(source, fields, remember, resolve_subject=False)
        else:
            _map_assertion_subject(source, remember, resolve_subject=False)
    labels = _reference_labels(things, sorted(references), target)
    for source in sources:
        # A field may appear in several certificates: normalize it only once.
        if source.get("active") is True:
            _map_source_references(source, fields, lambda reference: labels.get(reference, reference))
        else:
            _map_assertion_subject(source, lambda reference: labels.get(reference, reference))
        source["referenceFields"] = {certificate: fields[certificate] for certificate in source["validatedShapes"]}
    return canonical_current_state(sources)


def _current_name_heads(sources: list[dict]) -> list[dict]:
    heads: dict[str, dict] = {}
    for source in sources:
        name = source["name"]
        previous = heads.get(name)
        if previous is None:
            heads[name] = source
            continue
        active, prior_active = source.get("active"), previous.get("active")
        if type(active) is not bool or type(prior_active) is not bool or active and prior_active:
            raise CurrentStateError("SDK HEAD returned ambiguous current name holders")
        if active or prior_active:
            if active:
                heads[name] = source
            continue
        created, prior_created = source.get("createdAt"), previous.get("createdAt")
        if type(created) is not int or type(prior_created) is not int or created == prior_created:
            raise CurrentStateError("SDK HEAD cannot order retracted name holders")
        if created > prior_created:
            heads[name] = source
    return list(heads.values())


def _certified_fields(things: object, certificates: list[str]) -> dict[str, Mapping[str, object]]:
    if not certificates:
        return {}
    shape_references = []
    for certificate in certificates:
        if certificate.startswith("wh:"):
            origin, repository, name = certificate.split("/", 2)
            shape_references.append(f"{origin}/{repository}/{name if name.startswith('hub/shapes/') else 'hub/shapes/' + name}")
        else:
            shape_references.append(certificate if certificate.startswith("hub/shapes/") else f"hub/shapes/{certificate}")
    result = things.get_many(shape_references, include_retracted=True)  # type: ignore[attr-defined]
    if result.missing or len(result.items) != len(certificates):
        raise CurrentStateError("cannot read all certified Shape versions")
    fields: dict[str, Mapping[str, object]] = {}
    for reference, item in zip(certificates, result.items, strict=True):
        if item.kind != "shape" or not isinstance(item.data, Mapping):
            raise CurrentStateError(f"certified Shape {reference} is not a Shape definition")
        own_fields = item.data.get("fields")
        if not isinstance(own_fields, Mapping):
            raise CurrentStateError(f"certified Shape {reference} lacks field definitions")
        fields[reference] = own_fields
    return fields


def _reference_labels(things: object, references: list[str], target: str) -> dict[str, str]:
    if not references:
        return {}
    # Shape subjects already name a pinned Shape version; no Thing-name rewrite.
    shape_subjects = {
        ref for ref in references
        if (ref.split("/", 2)[-1] if ref.startswith("wh:") else ref).startswith("hub/shapes/")
    }
    requested = [ref for ref in references if ref not in shape_subjects]
    labels = {ref: ref for ref in shape_subjects}
    if not requested:
        return labels
    resolved = things.head_versions(requested)  # type: ignore[attr-defined]
    if resolved.missing or len(resolved.items) != len(requested):
        raise CurrentStateError("cannot resolve every current reference")
    for reference, item in zip(requested, resolved.items, strict=True):
        canonical = item.canonical_wref
        if not isinstance(canonical, str) or not canonical:
            raise CurrentStateError("resolved reference lacks a canonical identity")
        local_prefix = f"wh:{target}/"
        label = canonical[len(local_prefix):] if canonical.startswith(local_prefix) else canonical
        selector = _PIN.search(reference)
        labels[reference] = _PIN.sub("", label) + (selector.group() if selector else "")
    return labels


def _map_source_references(
    source: dict[str, object],
    shape_fields: Mapping[str, Mapping[str, object]],
    replace: Callable[[str], str],
    *, resolve_subject: bool = True,
) -> None:
    data = source.get("data")
    validated = source.get("validatedShapes")
    if not isinstance(data, dict) or not isinstance(validated, list):
        raise CurrentStateError("SDK row lacks certificate-aware data")

    def normalize(reference: str) -> str:
        selector = _PIN.search(reference)
        resolved = replace(reference)
        return resolved if selector is None or _PIN.search(resolved) else resolved + selector.group()

    try:
        original = deepcopy(data)
        for certificate in validated:
            fields = shape_fields.get(certificate)
            if fields is None:
                raise CurrentStateError(f"SDK row references unread certified Shape {certificate}")
            updated = deepcopy({name: original[name] for field in fields
                                if (name := field.removesuffix("?")) in original})
            map_reference_fields(updated, fields, normalize)
            _merge_reference_changes(data, original, updated)
        _map_assertion_subject(source, normalize, original=original, resolve_subject=resolve_subject)
    except ValueError as error:
        raise CurrentStateError(str(error)) from None


def _merge_reference_changes(destination: object, original: object, updated: object) -> None:
    # Each certificate reads the original body, including shared nested fields.
    for key in updated if isinstance(updated, dict) else range(len(updated)):
        if isinstance(updated[key], (dict, list)):
            _merge_reference_changes(destination[key], original[key], updated[key])
        elif updated[key] != original[key]:
            destination[key] = updated[key]


def _map_assertion_subject(
    source: dict[str, object], replace: Callable[[str], str], *, original: dict | None = None,
    resolve_subject: bool = True,
) -> None:
    indexed = source.get("aboutWref")
    if indexed is None:
        return
    if not isinstance(indexed, str) or not indexed:
        raise CurrentStateError("SDK assertion index lacks a valid subject")
    data = source.get("data")
    if not isinstance(data, dict):
        raise CurrentStateError("SDK assertion lacks object data")
    subject = replace(indexed)
    native = (original if original is not None else data).get("about")
    if "about" in (original if original is not None else data):
        if not isinstance(native, str):
            raise CurrentStateError("SDK assertion body conflicts with indexed subject")
        normalized_native = replace(native)
        if resolve_subject and normalized_native != subject:
            raise CurrentStateError("SDK assertion body conflicts with indexed subject")
    if not resolve_subject:
        return
    if "about" not in (original if original is not None else data):
        source["needsAssertionAdoption"] = True
    data["about"] = subject
    source["aboutWref"] = subject
    source["assertionSubject"] = subject


def canonical_current_state(rows: Iterable[Mapping[str, object]]) -> tuple[int, bytes]:
    """Normalize SDK rows without deriving declarations from certificate membership."""
    identities: set[str] = set()
    names: set[str] = set()
    records: list[dict[str, object]] = []
    for source in rows:
        source = deepcopy(dict(source))
        _map_assertion_subject(source, lambda reference: reference)
        metadata = source.get("metadata")
        durable_id = source.get("durableId")
        if durable_id is None and isinstance(metadata, Mapping):
            durable_id = metadata.get("durableId")
        if not isinstance(durable_id, str) or not durable_id:
            raise CurrentStateError("SDK row lacks a durable identity")
        if durable_id in identities:
            raise CurrentStateError("SDK HEAD returned duplicate current identities")
        name, shapes, data, version, active = (source.get(key) for key in ("name", "declaredShapes", "data", "version", "active"))
        if not isinstance(name, str) or not name or "@" in name or name in names:
            raise CurrentStateError("SDK HEAD returned invalid or duplicate current names")
        if not isinstance(shapes, list) or not all(isinstance(shape, str) and shape for shape in shapes) or len(set(shapes)) != len(shapes):
            raise CurrentStateError("SDK row lacks valid declared Shapes")
        if not isinstance(data, dict) or isinstance(version, bool) or not isinstance(version, int) or version < 1 or type(active) is not bool:
            raise CurrentStateError("SDK row is not a valid current-state record")
        identities.add(durable_id)
        names.add(name)
        record = {"name": name, "shapes": sorted(_PIN.sub("", shape) for shape in shapes), "data": data, "active": active, "version": version, "durableId": durable_id}
        if "validatedShapes" in source:
            validated = source["validatedShapes"]
            if not isinstance(validated, list) or not all(isinstance(shape, str) and _PIN.search(shape) for shape in validated) or len(set(validated)) != len(validated):
                raise CurrentStateError("SDK row lacks valid pinned Shape certificates")
            record["validatedShapes"] = sorted(validated)
        if "referenceFields" in source:
            record["referenceFields"] = source["referenceFields"]
        if "assertionSubject" in source:
            record["assertionSubject"] = source["assertionSubject"]
        if "needsAssertionAdoption" in source:
            record["needsAssertionAdoption"] = source["needsAssertionAdoption"]
        records.append(record)
    try:
        return len(records), canonical_jsonl(records, current=True)
    except KernelError as error:
        raise CurrentStateError(str(error)) from None


def _matches_scope(source: Mapping[str, object], pattern: str) -> bool:
    name = source.get("name")
    if not isinstance(name, str) or not name or "@" in name:
        raise CurrentStateError("SDK row lacks an unpinned Thing name")
    return matches_scope(name, pattern)
