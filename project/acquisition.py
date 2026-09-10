"""Acquisition boundary: nflverse releases -> frozen canonical snapshot.

``acquire()`` downloads the five nflverse dataset streams (raw transport stays
under the gitignored ``.cache/`` directory and is never committed; delete
within 30 days per the grounding contract), canonicalizes each stream under
``agentgm-nflverse-canonical-jsonl/v1``, reconciles the committed artifact
ledger (``artifacts/ledger.json``), stages changed canonical bytes under
``artifacts/staged/``, and returns one frozen JSON envelope for the transform.

Ledger semantics (makes desired state deterministic and unchanged recapture a
complete no-op): a stream whose canonical hash equals its ledger row leaves the
row byte-for-byte untouched (``acceptedAt`` is the FIRST acceptance time); a
changed hash stages the canonical bytes and rewrites the row with a fresh
``acceptedAt``.

Bootstrap rule: ``player-stats/2026`` and ``team-stats/2026`` are optionally
absent until nflverse first publishes them. An absent upstream asset (HTTP 404
or zero rows at the declared grain) yields NO artifact update and NO desired
records for that stream -- not an empty artifact. Once a stream has a ledger
row, upstream disappearance fails the run. The other three streams are
required and fail closed when unavailable, empty, or malformed.

Live safety: when a live WarmHub write could follow (``WH_TOKEN`` present, or
``NFLSTATS_REQUIRE_PUBLISHED_ARTIFACTS`` set), every ledger ``durableUri`` must
answer an HTTP HEAD with the exact canonical byte length; otherwise the run
fails closed with instructions to run ``scripts/publish_artifacts.sh``.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from collections.abc import Callable
from pathlib import Path
from tempfile import NamedTemporaryFile

import duckdb

from project.canonicalization import (
    MEDIA_TYPE,
    POLICY_NAME,
    SEASON,
    STREAMS,
    CanonicalStream,
    StreamSpec,
    canonicalize,
)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
LEDGER_PATH = _PROJECT_ROOT / "artifacts" / "ledger.json"
STAGED_DIR = _PROJECT_ROOT / "artifacts" / "staged"

# Durable-locator host decision (public GitHub Releases on this repository) is
# PENDING operator approval; see project/SPEC.md "Open decisions".
DEFAULT_DURABLE_URI_TEMPLATE = (
    "https://github.com/WJCorey/nfl-stats-sync/releases/download/"
    "artifacts/{slug}-{sha16}.jsonl"
)

_USER_AGENT = "nfl-stats-sync-mushroom/1.0 (+https://github.com/WJCorey/nfl-stats-sync)"


class AcquisitionError(RuntimeError):
    """The source cannot be frozen into a safe snapshot."""


def acquire() -> bytes:
    """Freeze the current nflverse state into one canonical snapshot."""

    return build_snapshot(
        capture=_capture_stream,
        ledger_path=LEDGER_PATH,
        staged_dir=STAGED_DIR,
        now=_utc_now,
        require_published=_publication_required(),
        head_ok=_http_head_ok,
    )


def build_snapshot(
    *,
    capture: Callable[[StreamSpec], tuple[list[dict[str, object]], str | None] | None],
    ledger_path: Path,
    staged_dir: Path,
    now: Callable[[], str],
    require_published: bool,
    head_ok: Callable[[str, int], bool],
) -> bytes:
    ledger = _read_ledger(ledger_path)
    unknown = sorted(set(ledger) - set(STREAMS))
    if unknown:
        raise AcquisitionError(
            f"ledger names streams this project no longer declares: {unknown}; "
            "migrate artifacts/ledger.json deliberately before running"
        )

    streams: dict[str, list[dict[str, object]]] = {}
    exclusions: dict[str, int] = {}
    absent: list[str] = []
    changed = False
    for stream, spec in STREAMS.items():
        captured = capture(spec)
        if captured is None:
            if spec.required:
                raise AcquisitionError(
                    f"required stream {stream} is unavailable or empty upstream"
                )
            if stream in ledger:
                raise AcquisitionError(
                    f"stream {stream} has an accepted artifact but is now absent "
                    "upstream; refusing to guess what that means"
                )
            absent.append(stream)  # bootstrap rule: no artifact, no records
            continue
        rows, published_at = captured
        canonical = canonicalize(stream, rows)
        row = ledger.get(stream)
        if row is not None and row.get("semanticSha256") == canonical.sha256:
            if row.get("byteLength") != canonical.byte_length:
                raise AcquisitionError(f"ledger entry for {stream} is internally inconsistent")
            # Unchanged recapture: the ledger row is deliberately untouched.
        else:
            _stage(staged_dir, spec, canonical)
            ledger[stream] = _ledger_row(spec, canonical, accepted_at=now(), published_at=published_at)
            changed = True
        streams[stream] = [dict(record) for record in canonical.records]
        exclusions[stream] = canonical.excluded_rows

    if changed:
        _write_ledger(ledger_path, ledger)
    if require_published:
        unpublished = [
            stream
            for stream in sorted(ledger)
            if not head_ok(str(ledger[stream]["durableUri"]), int(ledger[stream]["byteLength"]))
        ]
        if unpublished:
            raise AcquisitionError(
                "live synchronization requires published canonical artifacts; "
                f"unpublished durableUri for: {unpublished}. Run scripts/publish_artifacts.sh, "
                "commit artifacts/ledger.json, then rerun."
            )

    envelope = {
        "complete": False,
        "policy": POLICY_NAME,
        "ledger": ledger,
        "streams": streams,
        "exclusions": exclusions,
        "absentStreams": sorted(absent),
    }
    return (
        json.dumps(envelope, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
        + b"\n"
    )


# --------------------------------------------------------------------------
# Ledger persistence
# --------------------------------------------------------------------------


def _read_ledger(path: Path) -> dict[str, dict[str, object]]:
    if not path.exists():
        return {}
    try:
        ledger = json.loads(path.read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise AcquisitionError(f"cannot read artifact ledger {path}") from error
    if not isinstance(ledger, dict) or not all(isinstance(row, dict) for row in ledger.values()):
        raise AcquisitionError("artifact ledger must map stream names to objects")
    return ledger


def _write_ledger(path: Path, ledger: dict[str, dict[str, object]]) -> None:
    payload = json.dumps(ledger, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", dir=path.parent, delete=False, encoding="utf-8") as temporary:
        temporary_path = Path(temporary.name)
        temporary.write(payload)
        temporary.flush()
        os.fsync(temporary.fileno())
    temporary_path.replace(path)


def _ledger_row(
    spec: StreamSpec, canonical: CanonicalStream, *, accepted_at: str, published_at: str | None
) -> dict[str, object]:
    sha16 = canonical.sha256[:16]
    template = os.environ.get("NFLSTATS_DURABLE_URI_TEMPLATE", DEFAULT_DURABLE_URI_TEMPLATE)
    row: dict[str, object] = {
        "semanticSha256": canonical.sha256,
        "byteLength": canonical.byte_length,
        "recordCount": len(canonical.records),
        "mediaType": MEDIA_TYPE,
        "acceptedAt": accepted_at,
        "originalUrl": spec.original_url,
        "durableUri": template.format(slug=spec.slug, sha16=sha16, sha256=canonical.sha256),
        "canonicalizationPolicy": POLICY_NAME,
    }
    if published_at:
        row["sourcePublishedAt"] = published_at
    return row


def _stage(staged_dir: Path, spec: StreamSpec, canonical: CanonicalStream) -> None:
    path = staged_dir / spec.slug / f"{canonical.sha256}.jsonl"
    if path.exists():
        if path.read_bytes() != canonical.payload:
            raise AcquisitionError(f"staged artifact {path} holds different bytes")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=path.parent, delete=False) as temporary:
        temporary_path = Path(temporary.name)
        temporary.write(canonical.payload)
        temporary.flush()
        os.fsync(temporary.fileno())
    temporary_path.replace(path)


# --------------------------------------------------------------------------
# Live capture (network); raw transport stays in the gitignored cache
# --------------------------------------------------------------------------


def _utc_now() -> str:
    import datetime

    return (
        datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()
        .replace("+00:00", "Z")
    )


def _publication_required() -> bool:
    flag = os.environ.get("NFLSTATS_REQUIRE_PUBLISHED_ARTIFACTS", "").strip().lower()
    if flag in {"1", "true", "yes"}:
        return True
    # A present WarmHub credential means a live write may follow this acquire.
    return bool(os.environ.get("WH_TOKEN"))


def _cache_dir() -> Path:
    return Path(os.environ.get("NFLSTATS_CACHE_DIR", str(_PROJECT_ROOT / ".cache")))


def _download(url: str, destination: Path) -> bool:
    """Fetch one transport asset; False means the publisher has no such asset."""

    request = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            destination.parent.mkdir(parents=True, exist_ok=True)
            with NamedTemporaryFile(dir=destination.parent, delete=False) as temporary:
                temporary_path = Path(temporary.name)
                while chunk := response.read(1024 * 1024):
                    temporary.write(chunk)
                temporary.flush()
                os.fsync(temporary.fileno())
            temporary_path.replace(destination)
            return True
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return False
        raise AcquisitionError(f"transport fetch failed for {url}: HTTP {error.code}") from error
    except (urllib.error.URLError, OSError) as error:
        raise AcquisitionError(f"transport fetch failed for {url}") from error


def _http_head_ok(url: str, expected_length: int) -> bool:
    request = urllib.request.Request(url, method="HEAD", headers={"User-Agent": _USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            if response.status != 200:
                return False
            declared = response.headers.get("Content-Length")
            return declared is None or int(declared) == expected_length
    except (urllib.error.URLError, OSError, ValueError):
        return False


def _asset_published_at(spec: StreamSpec) -> str | None:
    """Best-effort GitHub release asset updated_at; never fails the run."""

    if spec.release_api is None:
        return None
    request = urllib.request.Request(
        spec.release_api, headers={"User-Agent": _USER_AGENT, "Accept": "application/vnd.github+json"}
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            release = json.loads(response.read())
    except (urllib.error.URLError, OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    assets = release.get("assets") if isinstance(release, dict) else None
    if not isinstance(assets, list):
        return None
    for asset in assets:
        if isinstance(asset, dict) and asset.get("name") == spec.asset_name:
            updated = asset.get("updated_at")
            return updated if isinstance(updated, str) and updated else None
    return None


def _read_rows(spec: StreamSpec, path: Path) -> list[dict[str, object]]:
    connection = duckdb.connect(":memory:")
    try:
        if spec.transport == "parquet":
            relation = "read_parquet(?)"
        elif spec.transport == "csv-r-na":
            # R-style CSV transport: empty fields and the literal 'NA' are the
            # writer's missing-value markers, decoded to null (a transport
            # decision declared by the canonicalization policy).
            relation = "read_csv(?, header=true, nullstr=['', 'NA'])"
        else:  # pragma: no cover - specs are static
            raise AcquisitionError(f"unknown transport {spec.transport}")
        query = f"SELECT * FROM {relation}"
        if spec.season_column is not None:
            query += f" WHERE {spec.season_column} = {int(SEASON)}"
        cursor = connection.execute(query, [str(path)])
        columns = [description[0] for description in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    except duckdb.Error as error:
        raise AcquisitionError(f"cannot parse transport for {spec.stream}") from error
    finally:
        connection.close()


def _capture_stream(spec: StreamSpec) -> tuple[list[dict[str, object]], str | None] | None:
    destination = _cache_dir() / spec.asset_name
    if not _download(spec.original_url, destination):
        return None
    rows = _read_rows(spec, destination)
    if not rows:
        if spec.required:
            raise AcquisitionError(f"required stream {spec.stream} produced zero rows")
        return None  # optional stream not yet published at the declared grain
    return rows, _asset_published_at(spec)
