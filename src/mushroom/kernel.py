"""Small deterministic persistence and reconciliation kernel."""

from __future__ import annotations

import fnmatch
import hashlib
import json
import os
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Iterator, Literal

import duckdb
from warmhub.operations import to_backend_stream_operation

Kind = Literal["thing", "collection", "assertion"]
AbsencePolicy = Literal["preserve", "retract"]

_PHASE = {
    ("add", "thing"): 10,
    ("revise", "thing"): 20,
    ("add", "collection"): 30,
    ("revise", "collection"): 40,
    ("add", "assertion"): 50,
    ("revise", "assertion"): 60,
    ("retract", "assertion"): 70,
    ("retract", "collection"): 80,
    ("retract", "thing"): 90,
}
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

    def __post_init__(self) -> None:
        if (
            not isinstance(self.target, str)
            or not isinstance(self.pattern, str)
            or "/" not in self.target
            or "/" not in self.pattern
            or "@" in self.target
            or "@" in self.pattern
        ):
            raise KernelError("target and managed-scope pattern must be unpinned names")
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


def reconcile(desired_path: Path, current_path: Path, operations_path: Path, policy: ScopePolicy) -> PlanResult:
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
        _load_jsonl(connection, desired_path, policy, current=False)
        _load_jsonl(connection, current_path, policy, current=True)

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

        connection.execute("CREATE TABLE operations (phase INTEGER, wref VARCHAR, payload VARCHAR)")
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
        while rows := joined.fetchmany(1000):
            operations = []
            for wanted_json, existing_json, existing_is_managed in rows:
                wanted = json.loads(wanted_json) if wanted_json else None
                existing = json.loads(existing_json) if existing_json else None
                if wanted is not None and (existing is None or not existing["active"]):
                    disposition = "add"
                elif wanted is not None and existing is not None:
                    if (
                        wanted["kind"] != existing["kind"]
                        or wanted["shape"] != existing["shape"]
                    ):
                        raise KernelError(
                            f"active current identity changed kind or Shape: {wanted['wref']}"
                        )
                    if (
                        wanted["kind"] == "assertion"
                        and not _same_reference(wanted["about"], existing["about"])
                    ):
                        raise KernelError(
                            f"assertion subject is immutable for active identity {wanted['wref']}"
                        )
                    disposition = (
                        "revise" if not _same_state(wanted, existing) else "unchanged"
                    )
                elif not existing["active"] or not existing_is_managed or policy.absence == "preserve":
                    disposition = "preserved"
                else:
                    disposition = "retract"
                counts[disposition] += 1
                if disposition in {"add", "revise", "retract"}:
                    record = wanted or existing
                    operation = _operation(
                        disposition,
                        record,
                        version=existing["version"] if disposition != "add" else None,
                    )
                    operations.append(
                        (
                            _PHASE[(operation["operation"], operation["kind"])],
                            record["wref"],
                            canonical_json(operation).decode(),
                        )
                    )
            if operations:
                connection.executemany("INSERT INTO operations VALUES (?, ?, ?)", operations)
        digest = hashlib.sha256()
        byte_length = 0
        operations_path.parent.mkdir(parents=True, exist_ok=True)
        with NamedTemporaryFile(dir=operations_path.parent, delete=False) as temporary:
            temporary_path = Path(temporary.name)
            output_rows = connection.cursor().execute("SELECT payload FROM operations ORDER BY phase, wref")
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


def _load_jsonl(connection: duckdb.DuckDBPyConnection, path: Path, policy: ScopePolicy, *, current: bool) -> None:
    rows: list[tuple[object, ...]] = []
    for record in _records_from_jsonl(path):
        _validate_record(record, current=current)
        rows.append((record["wref"], canonical_json(record).decode(), *( [record["active"]] if current else []), matches_scope(record["wref"], policy.pattern)))
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
        wref = record["wref"]
        if wref in indexed:
            raise KernelError(f"duplicate record identity: {wref}")
        indexed[wref] = record
    return indexed


def _validate_record(record: dict[str, object], *, current: bool) -> None:
    if not isinstance(record, dict):
        raise KernelError("records must be objects")
    kind = record.get("kind")
    shape = record.get("shape")
    wref = record.get("wref")
    if kind not in {"thing", "collection", "assertion"}:
        raise KernelError("records may contain only things, collections, and assertions")
    if not isinstance(wref, str) or "/" not in wref or "@" in wref:
        raise KernelError("record wrefs must be unpinned names")
    if not isinstance(shape, str) or not shape or wref.split("/", 1)[0] != shape:
        raise KernelError("record shapes must match their wrefs")
    if current:
        version = record.get("version")
        if type(record.get("active")) is not bool or type(version) is not int or version < 1:
            raise KernelError("current records require active and a positive integer version")
    elif "active" in record or "version" in record:
        raise KernelError("desired state must be operation-neutral")
    if kind == "collection":
        if not isinstance(record.get("collectionType"), str) or not isinstance(record.get("members"), list):
            raise KernelError("collections require a type and members")
        if not all(_is_reference(member) for member in record["members"]):
            raise KernelError("collection members must be valid references")
    else:
        if not isinstance(record.get("data"), dict):
            raise KernelError("things and assertions require object data")
        if kind == "assertion":
            about = record.get("about")
            if not _is_reference(about):
                raise KernelError("assertions require a valid about reference")


def _is_reference(value: object) -> bool:
    match = _REFERENCE_WREF.fullmatch(value) if isinstance(value, str) else None
    return match is not None and "/" in match.group("base")


def _same_state(desired: dict[str, object], current: dict[str, object]) -> bool:
    if desired["kind"] == "collection":
        wanted, existing = desired["members"], current["members"]
        return (
            desired["collectionType"] == current["collectionType"]
            and isinstance(wanted, list)
            and isinstance(existing, list)
            and len(wanted) == len(existing)
            and all(_same_reference(left, right) for left, right in zip(wanted, existing, strict=True))
        )
    if desired["kind"] == "assertion":
        return desired["data"] == current["data"] and _same_reference(desired["about"], current["about"])
    return desired["data"] == current["data"]


def _same_reference(desired: object, current: object) -> bool:
    if desired == current:
        return True
    return isinstance(desired, str) and isinstance(current, str) and current.startswith(f"{desired}@v")


def _operation(verb: str, record: dict[str, object], *, version: object | None = None) -> dict[str, object]:
    operation: dict[str, object] = {
        "operation": verb,
        "kind": record["kind"],
        "name": record["wref"],
    }
    if version is not None and verb != "add":
        operation["expectedVersion"] = version
    if verb != "retract":
        if record["kind"] == "collection":
            operation["type"] = record["collectionType"]
            operation["members"] = record["members"]
            if verb == "add":
                operation["name"] = record["wref"].split("/", 1)[1]
        else:
            operation["data"] = record["data"]
            if record["kind"] == "assertion" and verb == "add":
                operation["about"] = record["about"]
    try:
        normalized = to_backend_stream_operation(operation)  # type: ignore[arg-type]
    except (TypeError, ValueError) as error:
        raise KernelError("operation is rejected by the pinned WarmHub SDK") from error
    if normalized != operation:
        raise KernelError("operation differs from the pinned WarmHub SDK wire contract")
    return operation
