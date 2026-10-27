"""Small deterministic persistence and reconciliation kernel."""

from __future__ import annotations

import fnmatch
import hashlib
import heapq
import json
import os
import re
from dataclasses import asdict, dataclass, field
from copy import deepcopy
from collections import Counter, defaultdict
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Iterator, Literal, Mapping

import duckdb
from warmhub.operations import to_backend_stream_operation
from warmhub.errors import WarmHubError

from mushroom.references import BUILTIN_REFERENCE_FIELDS, map_reference_fields, reference_bindings, shape_name

AbsencePolicy = Literal["preserve", "retract"]
_REFERENCE_WREF = re.compile(r"^(?P<base>[^@\s]+)(?:@v[1-9][0-9]*)?$")


class KernelError(ValueError):
    """An input cannot safely produce a synchronization plan."""


@dataclass(frozen=True, slots=True)
class ArtifactRef:
    sha256: str
    byte_length: int
    key: str


@dataclass(frozen=True, slots=True)
class ScopePolicy:
    target: str
    pattern: str
    complete: bool
    absence: AbsencePolicy
    allow_shape_changes: bool = field(default=False, kw_only=True)
    require_existing_records: bool = field(default=False, kw_only=True)

    def __post_init__(self) -> None:
        if (
            not isinstance(self.target, str)
            or not isinstance(self.pattern, str)
            or "/" not in self.target
            or not self.pattern
            or "@" in self.target
            or "@" in self.pattern
        ):
            raise KernelError("target and managed-scope pattern must be unpinned names")
        if type(self.allow_shape_changes) is not bool:
            raise KernelError("allow_shape_changes must be a boolean")
        if type(self.require_existing_records) is not bool:
            raise KernelError("require_existing_records must be a boolean")
        if type(self.complete) is not bool:
            raise KernelError("managed-scope coverage must be a boolean")
        if self.absence not in {"preserve", "retract"}:
            raise KernelError("absence policy must be preserve or retract")
        if self.absence == "retract" and not self.complete:
            raise KernelError("incomplete coverage cannot authorize retractions")


def matches_scope(wref: str, pattern: str) -> bool:
    """Match Mushroom's simple path scopes: literal segments, `*`, and `**`."""

    if not isinstance(wref, str) or not isinstance(pattern, str):
        raise KernelError("managed-scope match requires strings")
    if any(character in pattern for character in "?[]{}()!\\"):
        raise KernelError("managed scopes support only literal segments, * and **")
    names = tuple(wref.split("/"))
    patterns = tuple(pattern.split("/"))
    if any("*" in segment and segment not in {"*", "**"} for segment in patterns):
        raise KernelError("managed scopes support only literal segments, * and **")

    def match(name_index: int, pattern_index: int) -> bool:
        if pattern_index == len(patterns):
            return name_index == len(names)
        segment = patterns[pattern_index]
        if segment == "**":
            if pattern_index == len(patterns) - 1:
                return name_index < len(names) if pattern_index else True
            return any(match(index, pattern_index + 1) for index in range(name_index, len(names) + 1))
        return (
            name_index < len(names)
            and fnmatch.fnmatchcase(names[name_index], segment)
            and match(name_index + 1, pattern_index + 1)
        )

    return match(0, 0)


@dataclass(frozen=True, slots=True)
class PlanSummary:
    add_count: int
    revise_count: int
    retract_count: int
    unchanged_count: int
    preserved_count: int

    @property
    def operation_count(self) -> int:
        return self.add_count + self.revise_count + self.retract_count


@dataclass(frozen=True, slots=True)
class PlanResult:
    summary: PlanSummary
    sha256: str
    byte_length: int


class LocalArtifactStore:
    """Content-addressed files plus small atomic named references."""

    def __init__(self, root: Path) -> None:
        self.root = root.resolve()

    def persist(self, payload: bytes) -> ArtifactRef:
        digest = hashlib.sha256(payload).hexdigest()
        key = f"objects/sha256/{digest}"
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            if path.read_bytes() != payload:
                raise KernelError("content-addressed artifact contains different bytes")
        else:
            with NamedTemporaryFile(dir=path.parent, delete=False) as temporary:
                temporary_path = Path(temporary.name)
                temporary.write(payload)
                temporary.flush()
                os.fsync(temporary.fileno())
            try:
                os.link(temporary_path, path)
            except FileExistsError:
                if path.read_bytes() != payload:
                    raise KernelError("artifact changed during persistence")
            finally:
                temporary_path.unlink(missing_ok=True)
        return ArtifactRef(digest, len(payload), key)

    def publish(self, name: str, reference: ArtifactRef) -> None:
        path = self.root / "refs" / f"{name}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = canonical_json(asdict(reference))
        with NamedTemporaryFile(dir=path.parent, delete=False) as temporary:
            temporary_path = Path(temporary.name)
            temporary.write(payload)
            temporary.flush()
            os.fsync(temporary.fileno())
        try:
            temporary_path.replace(path)
        finally:
            temporary_path.unlink(missing_ok=True)

    def resolve(self, name: str) -> tuple[ArtifactRef, bytes]:
        reference = self.reference(name)
        payload = (self.root / reference.key).read_bytes()
        if len(payload) != reference.byte_length or hashlib.sha256(payload).hexdigest() != reference.sha256:
            raise KernelError("persisted artifact identity is inconsistent")
        return reference, payload

    def reference(self, name: str) -> ArtifactRef:
        return ArtifactRef(**json.loads((self.root / "refs" / f"{name}.json").read_bytes()))

    def copy(self, reference: ArtifactRef, target: Path) -> None:
        """Verify and copy a persisted artifact for an ordinary file consumer."""

        source = self.root / reference.key
        digest = hashlib.sha256()
        byte_length = 0
        target.parent.mkdir(parents=True, exist_ok=True)
        with source.open("rb") as stream, NamedTemporaryFile(
            dir=target.parent, delete=False
        ) as temporary:
            temporary_path = Path(temporary.name)
            try:
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(chunk)
                    byte_length += len(chunk)
                    temporary.write(chunk)
                temporary.flush()
                os.fsync(temporary.fileno())
                if (
                    digest.hexdigest() != reference.sha256
                    or byte_length != reference.byte_length
                ):
                    raise KernelError("persisted artifact identity is inconsistent")
                temporary_path.replace(target)
            finally:
                temporary_path.unlink(missing_ok=True)


def canonical_json(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode() + b"\n"


def canonical_jsonl(
    records: list[dict[str, object]], *, current: bool = False
) -> bytes:
    by_wref = _index(records, current=current)
    return b"".join(canonical_json(by_wref[wref]) for wref in sorted(by_wref))


def parse_jsonl(payload: bytes) -> list[dict[str, object]]:
    return [json.loads(line) for line in payload.splitlines() if line.strip()]


def reconcile(
    desired_path: Path, current_path: Path, operations_path: Path, policy: ScopePolicy,
    *, reference_fields: Mapping[str, Mapping[str, object]] | None = None,
) -> PlanResult:
    """Plan canonical JSONL files through DuckDB without materializing either input."""

    if not all(isinstance(path, Path) for path in (desired_path, current_path, operations_path)):
        raise KernelError("reconciliation requires JSONL paths")
    if not isinstance(policy, ScopePolicy):
        raise KernelError("reconciliation requires a scope policy")
    try:
        if operations_path.resolve() in {desired_path.resolve(), current_path.resolve()}:
            raise KernelError("operation plan path must not be an input path")
    except OSError as error:
        raise KernelError("cannot resolve reconciliation paths") from error
    try:
        operations_path.unlink(missing_ok=True)
    except OSError as error:
        raise KernelError(f"cannot clear operation plan {operations_path}") from error
    connection = duckdb.connect(":memory:")
    temporary_path: Path | None = None
    try:
        connection.execute(
            """
            CREATE TABLE desired (
                wref VARCHAR,
                record_json VARCHAR,
                managed BOOLEAN
            );
            CREATE TABLE current (
                wref VARCHAR,
                record_json VARCHAR,
                active BOOLEAN,
                managed BOOLEAN
            );
            """
        )
        for shape in reference_fields or {}:
            if shape_name(shape) in BUILTIN_REFERENCE_FIELDS:
                raise KernelError(f"SHAPE_FIELDS cannot override builtin Shape {shape}")
        _load_jsonl(connection, desired_path, policy, current=False, reference_fields=reference_fields)
        _load_jsonl(connection, current_path, policy, current=True)

        if policy.require_existing_records and not connection.execute(
            "SELECT 1 FROM current WHERE active AND managed LIMIT 1"
        ).fetchone():
            raise KernelError("migration requires existing active records in the managed scope")
        hints = connection.cursor().execute(
            "SELECT d.wref, d.record_json, c.record_json FROM desired d JOIN current c "
            "ON substr(d.wref, strpos(d.wref, '/') + 1) = c.wref "
            "WHERE strpos(d.wref, '/') > 0 AND c.active "
            "AND NOT EXISTS (SELECT 1 FROM current holder WHERE holder.wref = d.wref AND holder.active) "
            "ORDER BY d.wref"
        )
        while hinted_rows := hints.fetchmany(1000):
            for hinted_name, wanted_json, current_json in hinted_rows:
                wanted, current = json.loads(wanted_json), json.loads(current_json)
                hint = hinted_name.split("/", 1)[0]
                if hint in {shape_name(shape) for shape in wanted["shapes"]} and hint in {
                    shape_name(shape) for shape in (*current["shapes"], *current.get("validatedShapes", []))
                }:
                    raise KernelError(f"legacy Shape/name identity would duplicate active bare name: {hinted_name}")

        duplicate = connection.execute(
            """
            SELECT wref
            FROM (
                SELECT wref FROM desired GROUP BY wref HAVING count(*) > 1
                UNION ALL
                SELECT wref FROM current GROUP BY wref HAVING count(*) > 1
            )
            ORDER BY wref
            LIMIT 1
            """
        ).fetchone()
        if duplicate:
            raise KernelError(f"duplicate record identity: {duplicate[0]}")

        outside_scope = connection.execute(
            "SELECT wref FROM desired WHERE NOT managed ORDER BY wref LIMIT 1"
        ).fetchone()
        if outside_scope:
            raise KernelError(f"desired record is outside the managed scope: {outside_scope[0]}")

        connection.execute("CREATE TABLE operations (wref VARCHAR, payload VARCHAR)")
        joined = connection.cursor().execute(
            """
            SELECT
                d.record_json,
                c.record_json,
                c.managed
            FROM desired AS d
            FULL OUTER JOIN current AS c USING (wref)
            ORDER BY coalesce(d.wref, c.wref)
            """
        )
        counts = {"add": 0, "revise": 0, "retract": 0, "unchanged": 0, "preserved": 0}
        dependencies: dict[str, tuple[str, set[str]]] = {}
        declaration_changes: set[str] = set()
        next_versions: dict[str, int] = {}
        surviving_references: dict[str, set[str]] = {}
        reference_selectors: dict[str, set[str]] = {}
        versioned_selectors: dict[str, set[str]] = {}
        while rows := joined.fetchmany(1000):
            operations = []
            for wanted_json, existing_json, existing_is_managed in rows:
                wanted = json.loads(wanted_json) if wanted_json else None
                existing = json.loads(existing_json) if existing_json else None
                if wanted is not None and existing is not None:
                    if set(wanted["shapes"]) != set(existing["shapes"]) and not policy.allow_shape_changes:
                        raise KernelError(f"declared Shapes changed; explicit migration required: {wanted['name']}")
                    if existing["active"]:
                        _require_binding_contract(wanted, existing, reference_fields)
                        wanted = _preserve_reference_pins(wanted, existing, reference_fields)
                if wanted is not None and (existing is None or not existing["active"]):
                    disposition = "add"
                elif wanted is not None and existing is not None:
                    if _is_assertion(existing) and wanted["data"].get("about") != existing["data"]["about"]:
                        raise KernelError(f"assertion subject is immutable for active identity {wanted['name']}")
                    disposition = (
                        "revise" if existing.get("needsAssertionAdoption", False) or not _same_state(wanted, existing) else "unchanged"
                    )
                elif not existing["active"] or not existing_is_managed or policy.absence == "preserve":
                    disposition = "preserved"
                else:
                    disposition = "retract"
                counts[disposition] += 1
                survivor = wanted or (existing if existing["active"] and disposition == "preserved" else None)
                survivor_refs = set()
                if survivor is not None:
                    survivor_refs = _typed_references(survivor, reference_fields, policy.target, existing)
                    floating = {reference for reference in survivor_refs if "@" not in reference}
                    if floating:
                        surviving_references[survivor["name"]] = floating
                if disposition in {"add", "revise", "retract"}:
                    record = wanted or existing
                    if disposition == "revise":
                        next_versions[record["name"]] = existing["version"] + 1
                        if set(record["shapes"]) != set(existing["shapes"]):
                            declaration_changes.add(record["name"])
                    operation = _operation(
                        disposition,
                        record,
                        version=existing["version"] if disposition != "add" else None,
                    )
                    reference_selectors[record["name"]] = (
                        survivor_refs if survivor is not None
                        else _typed_references(record, reference_fields, policy.target, existing)
                    )
                    versioned_selectors[record["name"]] = _typed_references(record, reference_fields, policy.target, existing, versioned_only=True)
                    dependencies[record["name"]] = (disposition, {
                        reference.split("@", 1)[0] for reference in reference_selectors[record["name"]]
                    })
                    operations.append(
                        (
                            record["name"],
                            canonical_json(operation).decode(),
                        )
                    )
            if operations:
                connection.executemany("INSERT INTO operations VALUES (?, ?)", operations)
        retractions = {name for name, (verb, _) in dependencies.items() if verb == "retract"}
        for name, references in surviving_references.items():
            if conflicts := references & retractions:
                raise KernelError(f"surviving Thing {name} references a scheduled retraction: {sorted(conflicts)[0]}")
        digest = hashlib.sha256()
        byte_length = 0
        operations_path.parent.mkdir(parents=True, exist_ok=True)
        with NamedTemporaryFile(dir=operations_path.parent, delete=False) as temporary:
            temporary_path = Path(temporary.name)
            connection.execute("CREATE TABLE operation_order (position INTEGER, wref VARCHAR)")
            order_rows = []
            for position, name in enumerate(_ordered_operations(dependencies, declaration_changes, next_versions, reference_selectors, versioned_selectors)):
                order_rows.append((position, name))
                if len(order_rows) == 1000:
                    connection.executemany("INSERT INTO operation_order VALUES (?, ?)", order_rows)
                    order_rows.clear()
            if order_rows:
                connection.executemany("INSERT INTO operation_order VALUES (?, ?)", order_rows)
            output_rows = connection.cursor().execute(
                "SELECT payload FROM operations JOIN operation_order USING (wref) ORDER BY position"
            )
            while rows := output_rows.fetchmany(1000):
                for (payload,) in rows:
                    encoded = payload.encode()
                    temporary.write(encoded)
                    digest.update(encoded)
                    byte_length += len(encoded)
            temporary.flush()
            os.fsync(temporary.fileno())
        temporary_path.replace(operations_path)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        connection.close()

    return PlanResult(PlanSummary(
        add_count=counts["add"],
        revise_count=counts["revise"],
        retract_count=counts["retract"],
        unchanged_count=counts["unchanged"],
        preserved_count=counts["preserved"],
    ), digest.hexdigest(), byte_length)


def _load_jsonl(connection: duckdb.DuckDBPyConnection, path: Path, policy: ScopePolicy, *, current: bool, reference_fields=None) -> None:
    rows: list[tuple[object, ...]] = []
    for record in _records_from_jsonl(path):
        _validate_record(record, current=current)
        if not current:
            for shape in record["shapes"]:
                name = shape_name(shape)
                definition = (reference_fields or {}).get(shape, (reference_fields or {}).get(name))
                if definition is None and name not in BUILTIN_REFERENCE_FIELDS:
                    raise KernelError(f"desired identity {record['name']} requires reviewed effective SHAPE_FIELDS for {shape}")
                if definition is not None and not isinstance(definition, Mapping):
                    raise KernelError(f"effective SHAPE_FIELDS must contain field definitions for {shape}")
        rows.append((record["name"], canonical_json(record).decode(), *( [record["active"]] if current else []), matches_scope(record["name"], policy.pattern)))
        if len(rows) == 1000:
            connection.executemany("INSERT INTO current VALUES (?, ?, ?, ?)" if current else "INSERT INTO desired VALUES (?, ?, ?)", rows)
            rows.clear()
    if rows:
        connection.executemany("INSERT INTO current VALUES (?, ?, ?, ?)" if current else "INSERT INTO desired VALUES (?, ?, ?)", rows)


def _records_from_jsonl(path: Path) -> Iterator[dict[str, object]]:
    try:
        with path.open("rb") as stream:
            for number, line in enumerate(stream, 1):
                if line.strip():
                    value = json.loads(line)
                    if not isinstance(value, dict):
                        raise KernelError("records must be objects")
                    yield value
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise KernelError(f"invalid JSONL input {path}") from error


def _index(records: list[dict[str, object]], *, current: bool) -> dict[str, dict[str, object]]:
    indexed: dict[str, dict[str, object]] = {}
    for record in records:
        _validate_record(record, current=current)
        wref = record["name"]
        if wref in indexed:
            raise KernelError(f"duplicate record identity: {wref}")
        indexed[wref] = record
    return indexed


def _valid_name(value: object) -> bool:
    return isinstance(value, str) and bool(value) and not any(
        character.isspace() or character == "@" for character in value
    ) and all(part not in {"", ".", ".."} for part in value.split("/"))


def _validate_record(record: dict[str, object], *, current: bool) -> None:
    if not isinstance(record, dict):
        raise KernelError("records must be objects")
    if any(key in record for key in ("kind", "shape", "wref", "members", "about", "collectionType")):
        raise KernelError("legacy records require an explicit migration to name, shapes, data")
    if isinstance(record.get("name"), str) and record["name"].startswith("hub/shapes/"):
        raise KernelError("Shape mutations are outside the Thing planner")
    if not _valid_name(record.get("name")) or record["name"].startswith("wh:"):
        raise KernelError("record names must be unpinned names without empty path segments")
    shapes = record.get("shapes")
    if not isinstance(shapes, list) or not all(_valid_name(shape) for shape in shapes) or len(shapes) != len(set(shapes)):
        raise KernelError("declared Shapes must be a list of distinct unpinned names")
    if not isinstance(record.get("data"), dict):
        raise KernelError("Things require object data")
    if current:
        certificates = record.get("validatedShapes", [])
        if not isinstance(certificates, list) or not all(_is_reference(shape) for shape in certificates):
            raise KernelError("validatedShapes must be a list of valid Shape references")
        evidence = record.get("referenceFields", {})
        if not isinstance(evidence, dict) or any(
            certificate not in certificates or not isinstance(fields, dict)
            for certificate, fields in evidence.items()
        ):
            raise KernelError("referenceFields must map validated Shape certificates to field definitions")
        if "needsAssertionAdoption" in record:
            if type(record["needsAssertionAdoption"]) is not bool or record["needsAssertionAdoption"] and "assertionSubject" not in record:
                raise KernelError("needsAssertionAdoption must be a boolean with indexed assertionSubject evidence")
        if "assertionSubject" in record:
            subject = record["assertionSubject"]
            if not _is_reference(subject) or subject != record["data"].get("about"):
                raise KernelError("assertionSubject must match the normalized about reference")
        if "durableId" in record and (not isinstance(record["durableId"], str) or not record["durableId"]):
            raise KernelError("durableId must be a nonempty string")
        version = record.get("version")
        if type(record.get("active")) is not bool or type(version) is not int or version < 1:
            raise KernelError("current records require active and a positive integer version")
    elif any(key in record for key in ("active", "version", "durableId", "validatedShapes", "referenceFields", "assertionSubject", "needsAssertionAdoption")):
        raise KernelError("desired state must be operation-neutral")
    if _is_assertion(record) and not _is_reference(record["data"].get("about")):
        raise KernelError("assertions require a valid about reference")


def _is_reference(value: object) -> bool:
    match = _REFERENCE_WREF.fullmatch(value) if isinstance(value, str) else None
    return match is not None and _valid_name(match.group("base"))


def _is_assertion(record: dict[str, object]) -> bool:
    return "assertionSubject" in record or any(shape_name(shape) == "Assertion" for shape in (
        *record["shapes"], *record.get("validatedShapes", [])
    ))


def _same_state(desired: dict[str, object], current: dict[str, object]) -> bool:
    if set(desired["shapes"]) != set(current["shapes"]):
        return False
    wanted, existing = desired["data"], current["data"]
    if wanted.keys() != existing.keys():
        return False
    structural = _structural_fields(desired, current)
    return all(
        _same_structural_value(key, wanted[key], existing[key], desired, current) if key in structural
        else wanted[key] == existing[key]
        for key in wanted
    )


def _structural_fields(desired: dict[str, object], current: dict[str, object]) -> set[str]:
    fields = {"about"} if _is_assertion(desired) or _is_assertion(current) else set()
    for shape in {*desired["shapes"], *current.get("validatedShapes", [])}:
        fields.update(BUILTIN_REFERENCE_FIELDS.get(shape_name(shape), {}))
    return fields


def _unordered_field(field: str, *records: dict[str, object]) -> bool:
    shapes = {shape_name(shape) for record in records for shape in (
        *record["shapes"], *record.get("validatedShapes", [])
    )}
    return (field == "members" and "Set" in shapes) or (field == "ends" and "Bond" in shapes)


def _preserve_unordered_pins(desired: list[str], current: list[str], *, distinct: bool) -> list[str]:
    wanted = list(dict.fromkeys(desired)) if distinct else list(desired)
    remaining = Counter(list(dict.fromkeys(current)) if distinct else current)
    unmatched = defaultdict(list)
    # Explicit selectors and exact identities claim their matches first.
    for index, reference in enumerate(wanted):
        if remaining[reference]:
            remaining[reference] -= 1
        elif "@" not in reference:
            unmatched[reference].append(index)
    candidates = defaultdict(list)
    for reference, count in remaining.items():
        if count:
            candidates[reference.split("@", 1)[0]].extend([reference] * count)
    for identity, indices in unmatched.items():
        matches = candidates[identity]
        if len(indices) < len(matches) and len(set(matches)) > 1:
            raise KernelError(f"ambiguous historical collection reference; use an explicit selector: {identity}")
        for index, reference in zip(indices, sorted(matches)):
            wanted[index] = reference
    return wanted


def _same_structural_value(field: str, desired: object, current: object, *records: dict[str, object], allow_pin: bool = False) -> bool:
    if not _unordered_field(field, *records) or not isinstance(desired, list) or not isinstance(current, list):
        return _same_reference_value(desired, current) if allow_pin else desired == current
    if not all(isinstance(reference, str) for reference in [*desired, *current]):
        return False
    if field == "members":
        return set(desired) == set(current)
    return Counter(desired) == Counter(current)


def _require_binding_contract(desired, current, reference_fields):
    if set(desired["shapes"]) != set(current["shapes"]):
        return
    certificates = current.get("validatedShapes", [])
    if not certificates:
        return
    observed = current.get("referenceFields", {})
    if any(shape_name(certificate) not in BUILTIN_REFERENCE_FIELDS and certificate not in observed for certificate in certificates):
        raise KernelError("observed Shape contracts are incomplete; reviewed schema migration required")
    if all(shape_name(shape) in BUILTIN_REFERENCE_FIELDS for shape in desired["shapes"]):
        return
    old = {}
    new = {}
    for certificate in certificates:
        definition = observed.get(certificate, BUILTIN_REFERENCE_FIELDS.get(shape_name(certificate), {}))
        for path, modes in reference_bindings(definition).items():
            old[path] = old.get(path, modes) & modes
    if _is_assertion(current):
        old.setdefault(("about",), frozenset({"versioned"}))
    for definition in _reference_definitions(desired, reference_fields, current):
        for path, modes in reference_bindings(definition).items():
            new[path] = new.get(path, modes) & modes
    if any(new.get(path) != modes for path, modes in old.items()):
        raise KernelError("observed reference bindings differ from SHAPE_FIELDS; reviewed schema migration required")


def _reference_definitions(record: dict[str, object], reference_fields, evidence=None):
    # Desired writes use approved effective contracts, never a historical certificate.
    if "active" not in record:
        for shape in sorted(record["shapes"]):
            name = shape_name(shape)
            if name in BUILTIN_REFERENCE_FIELDS and evidence is not None:
                observed = [definition for certificate, definition in evidence.get("referenceFields", {}).items() if shape_name(certificate) == name]
                if observed:
                    yield from observed
                    continue
            definition = (reference_fields or {}).get(shape, (reference_fields or {}).get(name, BUILTIN_REFERENCE_FIELDS.get(name)))
            if definition is not None:
                yield definition
        return
    certified = record.get("referenceFields", {})
    yield from certified.values()
    certified_names = {shape_name(certificate) for certificate in certified}
    for shape in sorted({*record["shapes"], *record.get("validatedShapes", [])}):
        name = shape_name(shape)
        if name in certified_names:
            continue
        definition = (reference_fields or {}).get(shape, (reference_fields or {}).get(name, BUILTIN_REFERENCE_FIELDS.get(name)))
        if definition is not None:
            yield definition


def _preserve_reference_pins(desired, current, reference_fields):
    wanted = {**desired, "data": deepcopy(desired["data"])}
    definitions = list(_reference_definitions(wanted, reference_fields, current))
    # Collections match structurally before the ordinary positional schema walk.
    for field in _structural_fields(wanted, current):
        bindings = set()
        field_defined = False
        for definition in definitions:
            spec = definition.get(field)
            field_defined = field_defined or spec is not None
            if isinstance(spec, list):
                spec = spec[0] if spec else None
            elif isinstance(spec, dict) and spec.get("type") == "array":
                spec = spec.get("items")
            if isinstance(spec, dict) and spec.get("type", "").removesuffix("?") == "wref":
                bindings.add(spec.get("binding", "versioned"))
            elif isinstance(spec, str) and spec.removesuffix("?") == "wref":
                bindings.add("versioned")
        binding = "versioned" if "versioned" in bindings or not field_defined else "identity" if "identity" in bindings else "either"
        if field not in wanted["data"] or field not in current["data"]:
            continue
        desired_value, current_value = wanted["data"][field], current["data"][field]
        if (
            binding == "versioned" and _unordered_field(field, wanted, current)
            and isinstance(desired_value, list) and isinstance(current_value, list)
            and all(isinstance(reference, str) for reference in [*desired_value, *current_value])
        ):
            preserved = _preserve_unordered_pins(desired_value, current_value, distinct=field == "members")
            wanted["data"][field] = deepcopy(current_value) if _same_structural_value(
                field, preserved, current_value, wanted, current
            ) else preserved
        elif _same_structural_value(field, desired_value, current_value, wanted, current, allow_pin=binding == "versioned"):
            wanted["data"][field] = deepcopy(current_value)
    try:
        for definition in definitions:
            map_reference_fields(wanted["data"], definition, lambda reference: reference, current=current["data"])
    except ValueError as error:
        raise KernelError(f"invalid typed reference data for {wanted['name']}") from error
    return wanted


def _same_reference_value(desired: object, current: object) -> bool:
    if isinstance(desired, list) and isinstance(current, list):
        return len(desired) == len(current) and all(
            _same_reference_value(left, right) for left, right in zip(desired, current, strict=True)
        )
    return _same_reference(desired, current)


def _same_reference(desired: object, current: object) -> bool:
    if desired == current:
        return True
    return isinstance(desired, str) and isinstance(current, str) and current.startswith(f"{desired}@v") and _is_reference(current)


def _operation(verb: str, record: dict[str, object], *, version: object | None = None) -> dict[str, object]:
    operation: dict[str, object] = {"operation": verb, "name": record["name"]}
    if version is not None and verb != "add":
        operation["expectedVersion"] = version
    if verb != "retract":
        operation.update(shapes=sorted(record["shapes"]), data=record["data"])
    try:
        normalized = to_backend_stream_operation(operation)  # type: ignore[arg-type]
    except (TypeError, ValueError, WarmHubError) as error:
        raise KernelError("operation is rejected by the pinned WarmHub SDK") from error
    if normalized != operation:
        raise KernelError("operation differs from the pinned WarmHub SDK wire contract")
    return operation


def _typed_references(
    record: dict[str, object], reference_fields: Mapping[str, Mapping[str, object]] | None,
    target: str, evidence: dict[str, object] | None = None, *, versioned_only: bool = False,
) -> set[str]:
    references: set[str] = set()
    def remember(reference: str) -> str:
        if not _is_reference(reference):
            raise KernelError(f"invalid typed reference in {record['name']}: {reference}")
        references.add(reference.removeprefix(f"wh:{target}/"))
        return reference
    try:
        for definition in _reference_definitions(record, reference_fields, evidence):
            map_reference_fields(record["data"], definition, remember, binding_filter={"versioned"} if versioned_only else None)
        if _is_assertion(record) or evidence is not None and _is_assertion(evidence):
            about = record["data"].get("about")
            if not _is_reference(about):
                raise KernelError("assertions require a valid about reference")
            remember(about)
    except ValueError as error:
        raise KernelError(f"invalid typed reference data for {record['name']}") from error
    return references


def _ordered_operations(
    dependencies: Mapping[str, tuple[str, set[str]]], declaration_changes: set[str],
    next_versions: Mapping[str, int], reference_selectors: Mapping[str, set[str]],
    versioned_selectors: Mapping[str, set[str]],
) -> Iterator[str]:
    """Keep only the typed dependency graph in memory; payloads remain in DuckDB."""
    prerequisites: dict[str, set[str]] = {name: set() for name in dependencies}
    dependents: dict[str, set[str]] = {name: set() for name in dependencies}
    for name, (verb, references) in dependencies.items():
        for reference in references & dependencies.keys():
            other_verb = dependencies[reference][0]
            if verb == "retract" and other_verb == "retract" and reference in reference_selectors[name] and reference != name:
                prerequisites[reference].add(name)
                dependents[name].add(reference)
            elif verb != "retract" and (
                other_verb == "add"
                or (reference in declaration_changes and reference in reference_selectors[name] and reference != name)
                or (reference in next_versions and (f"{reference}@v{next_versions[reference]}" in reference_selectors[name]
                    or reference != name and reference in versioned_selectors[name]))
            ):
                prerequisites[name].add(reference)
                dependents[reference].add(name)
    phases = {"add": 0, "revise": 1, "retract": 2}
    ready = [(phases[dependencies[name][0]], name) for name in dependencies if not prerequisites[name]]
    heapq.heapify(ready)
    count = 0
    while ready:
        _, name = heapq.heappop(ready)
        count += 1
        yield name
        for dependent in sorted(dependents[name]):
            prerequisites[dependent].remove(name)
            if not prerequisites[dependent]:
                heapq.heappush(ready, (phases[dependencies[dependent][0]], dependent))
    if count != len(dependencies):
        raise KernelError("operation reference dependency cycle cannot be applied safely")
