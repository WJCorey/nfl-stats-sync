"""Canonicalization policy ``agentgm-nflverse-canonical-jsonl/v1``.

This module is the reviewed, stable semantics contract named by every
``SourceArtifact.canonicalizationPolicy`` this project emits. It defines, per
logical nflverse dataset stream:

- the semantic projection (ALL source columns are kept, including the
  pure-analytics columns the downstream transform drops -- dropping them is a
  transform decision, not artifact canonicalization; only transport is
  excluded);
- the declared stream grain and season filter;
- the stable source-grain sort key, with null/empty/duplicate-key rejection;
- canonical value representation (nulls and NaN omitted; dates as ISO strings;
  integral floats emitted as integers so int/float parquet-type flapping never
  changes the hash; non-finite numbers rejected);
- RFC 8785/JCS-compatible canonical JSON serialization: sorted object keys,
  minimal separators, UTF-8, one record per LF-delimited line, final LF.

The canonical bytes are hashed with SHA-256 (``semanticSha256``). Any change to
a claim value changes the bytes and the hash; any transport permutation (row
order, column order, packaging) does not.

Versioning: behavior changes here require bumping POLICY_NAME to ``.../v2`` and
recording the migration in project/SPEC.md.
"""

from __future__ import annotations

import datetime
import hashlib
import json
import math
from dataclasses import dataclass, field

POLICY_NAME = "agentgm-nflverse-canonical-jsonl/v1"
MEDIA_TYPE = "application/x-ndjson"

# The one season this project manages (see project/SPEC.md, "Season scope").
SEASONS = (2026,)
SEASON = SEASONS[0]


class CanonicalizationError(ValueError):
    """Source input cannot support a deterministic canonical artifact."""


@dataclass(frozen=True)
class StreamSpec:
    """One logical dataset stream (one SourceArtifact Thing)."""

    stream: str            # SourceArtifact name suffix, e.g. "player-stats/2026"
    original_url: str      # credential-free publisher retrieval locator
    release_api: str | None  # GitHub API release URL for asset updated_at
    asset_name: str        # transport file name under the cache directory
    transport: str         # "parquet" | "csv-r-na" (R-style CSV: '' and 'NA' are null)
    required: bool         # False = optionally absent until first publication
    season_column: str | None  # column filtered to SEASON, or None (full table)
    key_columns: tuple[str, ...]  # declared stable source-grain sort key
    # Columns whose null/empty value places a row outside the declared stream
    # grain (excluded deterministically and counted), rather than failing.
    grain_null_exempt: tuple[str, ...] = field(default_factory=tuple)
    # Key columns compared numerically for ordering (value stays a string claim).
    numeric_key_columns: tuple[str, ...] = field(default_factory=tuple)

    @property
    def slug(self) -> str:
        return self.stream.replace("/", "-")


_NFLVERSE = "https://github.com/nflverse/nflverse-data/releases/download"
_NFLVERSE_API = "https://api.github.com/repos/nflverse/nflverse-data/releases/tags"

# All five streams ground Source/nflverse. URLs verified 2026-09-10 with HEAD
# requests (302 -> 200 at the CDN); ff_playerids is published by DynastyProcess
# via nflverse (nflreadr::load_ff_playerids) as a CSV in the repository tree --
# there is no release-asset parquet for it.
STREAMS: dict[str, StreamSpec] = {
    spec.stream: spec
    for spec in (
        StreamSpec(
            stream=f"player-stats/{SEASON}",
            original_url=f"{_NFLVERSE}/stats_player/stats_player_week_{SEASON}.parquet",
            release_api=f"{_NFLVERSE_API}/stats_player",
            asset_name=f"stats_player_week_{SEASON}.parquet",
            transport="parquet",
            required=False,  # absent until nflverse first publishes the season file
            season_column="season",
            key_columns=("season", "week", "player_id", "team"),
            grain_null_exempt=("player_id",),
        ),
        StreamSpec(
            stream=f"team-stats/{SEASON}",
            original_url=f"{_NFLVERSE}/stats_team/stats_team_week_{SEASON}.parquet",
            release_api=f"{_NFLVERSE_API}/stats_team",
            asset_name=f"stats_team_week_{SEASON}.parquet",
            transport="parquet",
            required=False,
            season_column="season",
            key_columns=("season", "week", "team"),
        ),
        StreamSpec(
            stream=f"schedules/{SEASON}",
            original_url=f"{_NFLVERSE}/schedules/games.parquet",
            release_api=f"{_NFLVERSE_API}/schedules",
            asset_name="games.parquet",
            transport="parquet",
            required=True,
            season_column="season",
            key_columns=("game_id",),
        ),
        StreamSpec(
            stream="players",
            original_url=f"{_NFLVERSE}/players/players.parquet",
            release_api=f"{_NFLVERSE_API}/players",
            asset_name="players.parquet",
            transport="parquet",
            required=True,
            season_column=None,
            key_columns=("gsis_id",),
        ),
        StreamSpec(
            stream="ff-playerids",
            original_url="https://github.com/dynastyprocess/data/raw/master/files/db_playerids.csv",
            release_api=None,  # repository file, not a release asset
            asset_name="db_playerids.csv",
            transport="csv-r-na",
            required=True,
            season_column=None,
            # mfl_id is DynastyProcess's primary key: non-null and unique
            # (profiled 2026-09-10: 12,492 rows, 0 nulls, 0 duplicates). Many
            # rows lack gsis_id ('NA'), so gsis_id cannot key this stream.
            key_columns=("mfl_id",),
            numeric_key_columns=("mfl_id",),
        ),
    )
}

REQUIRED_STREAMS = tuple(s for s, spec in STREAMS.items() if spec.required)
OPTIONAL_STREAMS = tuple(s for s, spec in STREAMS.items() if not spec.required)


@dataclass(frozen=True)
class CanonicalStream:
    stream: str
    records: tuple[dict[str, object], ...]
    payload: bytes
    sha256: str
    excluded_rows: int

    @property
    def byte_length(self) -> int:
        return len(self.payload)


def canonical_value(value: object) -> object:
    """Map one source claim value to its canonical JSON representation.

    Returns None for "no value" (JSON null, parquet null, float NaN), which the
    caller omits from the record. Rejects non-finite numbers and unknown types.
    """

    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return None  # parquet NaN and null both mean "no value" in nflverse outputs
        if math.isinf(value):
            raise CanonicalizationError("non-finite claim value cannot be canonicalized")
        if value.is_integer() and abs(value) <= 2**53:
            return int(value)  # JCS integral form; stable across int/float parquet reads
        return value
    if isinstance(value, str):
        return value
    if isinstance(value, datetime.datetime):
        return value.isoformat()
    if isinstance(value, datetime.date):
        return value.isoformat()
    raise CanonicalizationError(f"unsupported claim value type: {type(value).__name__}")


def canonical_record(row: dict[str, object]) -> dict[str, object]:
    """Project one source row: canonical values, nulls omitted, all columns kept."""

    record: dict[str, object] = {}
    for column, value in row.items():
        if not isinstance(column, str) or not column:
            raise CanonicalizationError("source columns must have non-empty names")
        canonical = canonical_value(value)
        if canonical is not None:
            record[column] = canonical
    return record


def record_line(record: dict[str, object]) -> bytes:
    """RFC 8785/JCS-compatible canonical JSON, one LF-terminated line."""

    return (
        json.dumps(
            record, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
        + b"\n"
    )


def _sort_key(spec: StreamSpec, record: dict[str, object], ordinal: int) -> tuple:
    key = []
    for column in spec.key_columns:
        value = record.get(column)
        if value is None or value == "":
            raise CanonicalizationError(
                f"{spec.stream}: record {ordinal} has a null/empty key column {column!r}"
            )
        if column in spec.numeric_key_columns:
            try:
                value = int(str(value))
            except ValueError as error:
                raise CanonicalizationError(
                    f"{spec.stream}: key column {column!r} is not numeric: {value!r}"
                ) from error
        key.append(value)
    return tuple(key)


def canonicalize(stream: str, rows: list[dict[str, object]]) -> CanonicalStream:
    """Produce the canonical semantic artifact for one stream.

    ``rows`` are complete source rows (every column). Row and column order do
    not matter; equivalent transport packaging canonicalizes identically.
    """

    spec = STREAMS.get(stream)
    if spec is None:
        raise CanonicalizationError(f"unknown stream: {stream}")
    excluded = 0
    keyed: list[tuple[tuple, dict[str, object]]] = []
    for ordinal, row in enumerate(rows):
        if not isinstance(row, dict):
            raise CanonicalizationError(f"{spec.stream}: record {ordinal} is not an object")
        record = canonical_record(row)
        if spec.season_column is not None:
            if record.get(spec.season_column) != SEASON:
                raise CanonicalizationError(
                    f"{spec.stream}: record {ordinal} is outside the declared season filter"
                )
        # Rows whose grain-null-exempt key is missing are outside the declared
        # stream grain (e.g. team-level aggregate rows in player_stats with no
        # player_id). They are excluded deterministically and counted.
        exempt = next(
            (
                column
                for column in spec.grain_null_exempt
                if record.get(column) is None or record.get(column) == ""
            ),
            None,
        )
        if exempt is not None:
            excluded += 1
            continue
        keyed.append((_sort_key(spec, record, ordinal), record))
    keyed.sort(key=lambda pair: pair[0])
    for (left_key, _), (right_key, _) in zip(keyed, keyed[1:]):
        if left_key == right_key:
            raise CanonicalizationError(
                f"{spec.stream}: duplicate source-grain key {left_key!r}"
            )
    records = tuple(record for _, record in keyed)
    payload = b"".join(record_line(record) for record in records)
    return CanonicalStream(
        stream=spec.stream,
        records=records,
        payload=payload,
        sha256=hashlib.sha256(payload).hexdigest(),
        excluded_rows=excluded,
    )
