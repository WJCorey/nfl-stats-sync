"""agentgm/nfl-stats naming authority and transform.

Keeps the WarmHub repo ``agentgm/nfl-stats`` fresh during the 2026 NFL season
from nflverse data. Identity formulas and transform decisions are ported
verbatim from the approved one-shot ingest (``ingest/build_jsonl.py`` in the
agentgm workspace); grounding provenance follows the grounding 4.0 contract
(Source/SourceArtifact). See ``project/SPEC.md`` for the reviewed decisions.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path

from project.canonicalization import (
    MEDIA_TYPE,
    POLICY_NAME,
    REQUIRED_STREAMS,
    SEASON,
    STREAMS,
    canonicalize,
)

TARGET = "example/mushroom-starter"

# Mushroom accepts exactly one managed-scope glob. The semantic managed scope
# of this project (see SPEC.md) is the union of six narrower families, which a
# single kernel glob cannot express, so the kernel scope is the whole
# repository and the narrower semantic scope is enforced here in
# ``_assert_project_scope`` on every desired record. ABSENCE_POLICY is
# "preserve", so the wide kernel scope can never authorize a retraction; its
# only live effect is a whole-repository current-state read.
MANAGED_SCOPE = "*/**"
ABSENCE_POLICY = "preserve"

_FIXTURES = Path(__file__).parent / "fixtures"

# Identifier grammars profiled from the real sources (2026-09-10): gsis ids are
# either modern "00-" + 7 digits or the legacy 3-letters + 6-digits ids that
# nflverse uses for pre-GSIS-era players (both already exist as Player names on
# prod). Malformed, missing, or empty identifiers are rejected, never coerced.
_GSIS_ID = re.compile(r"^(00-[0-9]{7}|[A-Z]{3}[0-9]{6})$")
_TEAM_ABBR = re.compile(r"^[A-Z]{2,3}$")
_GAME_ID = re.compile(r"^[0-9]{4}_[0-9]{2}_[A-Z]{2,3}_[A-Z]{2,3}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_HTTPS_URL = re.compile(r"^https://[^@\s#]+$")

SOURCE_WREF = "Source/nflverse"
# Verbatim current prod state of Source/nflverse@v1 (read back 2026-09-10); an
# unchanged emission keeps the Thing unrevised.
SOURCE_DATA = {
    "publisherName": "nflverse project (open-source NFL data community)",
    "systemName": (
        "nflverse-data automated releases via nflreadr loaders: load_player_stats, "
        "load_team_stats, load_schedules, load_players, load_ff_playerids"
    ),
    "authorityClass": "open-data-aggregator",
    "canonicalUrl": "https://github.com/nflverse/nflverse-data",
}


class DomainError(ValueError):
    """Source input cannot support a safe desired state."""


def target() -> str:
    return os.environ.get("MUSHROOM_TARGET", TARGET)


def source_fixture() -> Path:
    return _FIXTURES / "source.json"


def current_state_fixture() -> Path:
    return _FIXTURES / "current.jsonl"


# --------------------------------------------------------------------------
# Naming authority (formulas continue the identities already live on prod)
# --------------------------------------------------------------------------


def _require_id(value: object, pattern: re.Pattern[str], label: str) -> str:
    if not isinstance(value, str) or not value or pattern.fullmatch(value) is None:
        raise DomainError(f"invalid {label}: {value!r}")
    return value


def _require_week(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 22:
        raise DomainError(f"invalid week: {value!r}")
    return value


def _require_season(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value != SEASON:
        raise DomainError(f"season outside managed scope: {value!r}")
    return value


def player_wref(gsis_id: object) -> str:
    return f"Player/{_require_id(gsis_id, _GSIS_ID, 'gsis_id')}"


def team_abbr(value: object) -> str:
    return _require_id(value, _TEAM_ABBR, "team abbreviation")


def game_wref(game_id: object) -> str:
    return f"Game/{_require_id(game_id, _GAME_ID, 'game_id')}"


def player_game_stats_wref(season: object, week: object, gsis_id: object) -> str:
    gsis = _require_id(gsis_id, _GSIS_ID, "gsis_id")
    return f"PlayerGameStats/{_require_season(season)}/{_require_week(week):02d}/{gsis}"


def team_game_stats_wref(season: object, week: object, team: object) -> str:
    abbr = team_abbr(team)
    return f"TeamGameStats/{_require_season(season)}/{_require_week(week):02d}/{abbr}"


def source_artifact_wref(stream: object) -> str:
    if stream not in STREAMS:
        raise DomainError(f"unknown source artifact stream: {stream!r}")
    return f"SourceArtifact/{stream}"


def _assert_project_scope(wref: str) -> None:
    """Enforce the six-family semantic managed scope from SPEC.md."""

    if wref == SOURCE_WREF:
        return
    shape, _, rest = wref.partition("/")
    if shape == "SourceArtifact" and rest in STREAMS:
        return
    if shape == "Player" and "/" not in rest and rest:
        return
    if shape == "Game" and rest.startswith(f"{SEASON}_"):
        return
    if shape in {"PlayerGameStats", "TeamGameStats"} and rest.startswith(f"{SEASON}/"):
        return
    raise DomainError(f"desired record outside the approved managed scope: {wref}")


# --------------------------------------------------------------------------
# Field mappings ported verbatim from ingest/build_jsonl.py
# --------------------------------------------------------------------------

# Dropped pure-analytics columns (transform decision; they remain in the
# canonical artifacts): passing_epa, passing_cpoe, pacr, rushing_epa,
# receiving_epa, racr, target_share, air_yards_share, wopr.
PGS_INT = {
    "completions": "completions", "attempts": "attempts", "passing_yards": "passingYards",
    "passing_tds": "passingTds", "passing_interceptions": "passingInterceptions",
    "sacks_suffered": "sacksSuffered", "sack_yards_lost": "sackYardsLost",
    "sack_fumbles": "sackFumbles", "sack_fumbles_lost": "sackFumblesLost",
    "passing_air_yards": "passingAirYards", "passing_yards_after_catch": "passingYardsAfterCatch",
    "passing_first_downs": "passingFirstDowns", "passing_2pt_conversions": "passing2ptConversions",
    "carries": "carries", "rushing_yards": "rushingYards", "rushing_tds": "rushingTds",
    "rushing_fumbles": "rushingFumbles", "rushing_fumbles_lost": "rushingFumblesLost",
    "rushing_first_downs": "rushingFirstDowns", "rushing_2pt_conversions": "rushing2ptConversions",
    "receptions": "receptions", "targets": "targets", "receiving_yards": "receivingYards",
    "receiving_tds": "receivingTds", "receiving_fumbles": "receivingFumbles",
    "receiving_fumbles_lost": "receivingFumblesLost", "receiving_air_yards": "receivingAirYards",
    "receiving_yards_after_catch": "receivingYardsAfterCatch",
    "receiving_first_downs": "receivingFirstDowns",
    "receiving_2pt_conversions": "receiving2ptConversions",
    "special_teams_tds": "specialTeamsTds", "punt_returns": "puntReturns",
    "punt_return_yards": "puntReturnYards", "kickoff_returns": "kickoffReturns",
    "kickoff_return_yards": "kickoffReturnYards",
    "def_tackles_solo": "defTacklesSolo", "def_tackle_assists": "defTackleAssists",
    "def_tackles_for_loss": "defTacklesForLoss",
    "def_tackles_for_loss_yards": "defTacklesForLossYards",
    "def_qb_hits": "defQbHits", "def_interceptions": "defInterceptions",
    "def_interception_yards": "defInterceptionYards", "def_pass_defended": "defPassDefended",
    "def_tds": "defTds", "def_fumbles_forced": "defFumblesForced", "def_fumbles": "defFumbles",
    "def_safeties": "defSafeties",
    "fg_made": "fgMade", "fg_att": "fgAtt", "fg_missed": "fgMissed",
    "fg_blocked": "fgBlocked", "fg_long": "fgLong",
    "fg_made_0_19": "fgMade_0_19", "fg_made_20_29": "fgMade_20_29",
    "fg_made_30_39": "fgMade_30_39", "fg_made_40_49": "fgMade_40_49",
    "fg_made_50_59": "fgMade_50_59", "fg_made_60_": "fgMade_60_",
    "fg_missed_0_19": "fgMissed_0_19", "fg_missed_20_29": "fgMissed_20_29",
    "fg_missed_30_39": "fgMissed_30_39", "fg_missed_40_49": "fgMissed_40_49",
    "fg_missed_50_59": "fgMissed_50_59", "fg_missed_60_": "fgMissed_60_",
    "pat_made": "patMade", "pat_att": "patAtt", "pat_missed": "patMissed",
    "pat_blocked": "patBlocked",
    "fumble_recovery_own": "fumbleRecoveryOwn",
    "fumble_recovery_yards_own": "fumbleRecoveryYardsOwn",
    "fumble_recovery_opp": "fumbleRecoveryOpp",
    "fumble_recovery_yards_opp": "fumbleRecoveryYardsOpp",
    "fumble_recovery_tds": "fumbleRecoveryTds",
    "penalties": "penalties", "penalty_yards": "penaltyYards",
}
PGS_FLOAT = {
    "def_sacks": "defSacks", "def_sack_yards": "defSackYards",
    "fantasy_points": "fantasyPoints", "fantasy_points_ppr": "fantasyPointsPpr",
}
TGS_INT = dict(PGS_INT)
TGS_FLOAT = {"def_sacks": "defSacks", "def_sack_yards": "defSackYards"}


def _zero_int(value: object) -> int | None:
    # Zero-omission convention: zero-valued count stats are omitted; absence
    # == 0 for a count stat. A present field always carries its real value.
    if value is None or value == 0:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise DomainError(f"count stat is not an integer: {value!r}")
    return value


def _float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DomainError(f"stat is not numeric: {value!r}")
    return round(float(value), 2)


def _clean(data: dict[str, object]) -> dict[str, object]:
    # Ported op() cleaning: drop None and empty-string values.
    return {key: value for key, value in data.items() if value is not None and value != ""}


def _thing(shape: str, wref: str, data: dict[str, object]) -> dict[str, object]:
    return {"kind": "thing", "shape": shape, "wref": wref, "data": _clean(data)}


def _season_type(value: object) -> str:
    # Ported: schedules game_type is REG/WC/DIV/CON/SB; stats season_type is
    # REG/POST. Everything that is not REG is POST.
    if not isinstance(value, str) or not value:
        raise DomainError(f"invalid season/game type: {value!r}")
    return "REG" if value == "REG" else "POST"


def _string(value: object) -> str | None:
    return None if value is None else str(value)


# --------------------------------------------------------------------------
# Transform
# --------------------------------------------------------------------------


def transform(source: bytes) -> list[dict[str, object]]:
    envelope = _validated_envelope(source)
    ledger: dict[str, dict[str, object]] = envelope["ledger"]
    streams: dict[str, list[dict[str, object]]] = envelope["streams"]

    records: list[dict[str, object]] = [_thing("Source", SOURCE_WREF, dict(SOURCE_DATA))]
    for stream in sorted(ledger):
        records.append(_source_artifact_thing(stream, ledger[stream]))

    schedules = streams[f"schedules/{SEASON}"]
    records.extend(_build_games(schedules))
    records.extend(_build_players(streams["players"], streams["ff-playerids"]))
    lookup = _game_lookup(schedules)
    player_stats = streams.get(f"player-stats/{SEASON}")
    if player_stats is not None:
        records.extend(_build_player_game_stats(player_stats, lookup))
    team_stats = streams.get(f"team-stats/{SEASON}")
    if team_stats is not None:
        records.extend(_build_team_game_stats(team_stats, lookup))

    seen: set[str] = set()
    for record in records:
        wref = record["wref"]
        if wref in seen:
            raise DomainError(f"duplicate desired identity: {wref}")
        seen.add(wref)
        _assert_project_scope(wref)
    return records


def _validated_envelope(source: bytes) -> dict[str, object]:
    try:
        envelope = json.loads(source)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise DomainError("source snapshot is not valid JSON") from error
    if not isinstance(envelope, dict):
        raise DomainError("source snapshot must be an object")
    if envelope.get("complete") is not False:
        raise DomainError("this project's snapshots must declare complete=false")
    ledger, streams = envelope.get("ledger"), envelope.get("streams")
    if not isinstance(ledger, dict) or not isinstance(streams, dict):
        raise DomainError("source snapshot must carry ledger and streams objects")
    if set(ledger) != set(streams):
        raise DomainError("ledger and stream coverage disagree; snapshot is incoherent")
    unknown = sorted(set(ledger) - set(STREAMS))
    if unknown:
        raise DomainError(f"unknown streams in snapshot: {unknown}")
    missing = sorted(set(REQUIRED_STREAMS) - set(ledger))
    if missing:
        raise DomainError(f"required streams missing from snapshot: {missing}")
    for stream in sorted(ledger):
        row = ledger[stream]
        _validate_ledger_row(stream, row)
        rows = streams[stream]
        if not isinstance(rows, list):
            raise DomainError(f"stream {stream} is not a record list")
        canonical = canonicalize(stream, rows)
        if canonical.sha256 != row["semanticSha256"] or canonical.byte_length != row["byteLength"]:
            raise DomainError(
                f"stream {stream} does not match its ledger entry; snapshot is incoherent"
            )
    return envelope


def _validate_ledger_row(stream: str, row: object) -> None:
    if not isinstance(row, dict):
        raise DomainError(f"ledger entry for {stream} must be an object")
    digest = row.get("semanticSha256")
    if not isinstance(digest, str) or _SHA256.fullmatch(digest) is None:
        raise DomainError(f"ledger entry for {stream} lacks a lowercase sha256")
    byte_length = row.get("byteLength")
    if isinstance(byte_length, bool) or not isinstance(byte_length, int) or byte_length < 0:
        raise DomainError(f"ledger entry for {stream} lacks a byte length")
    for field in ("acceptedAt",):
        if not isinstance(row.get(field), str) or not row[field]:
            raise DomainError(f"ledger entry for {stream} lacks {field}")
    for field in ("originalUrl", "durableUri"):
        value = row.get(field)
        if not isinstance(value, str) or _HTTPS_URL.fullmatch(value) is None:
            raise DomainError(f"ledger entry for {stream} lacks a safe {field}")
    if row.get("canonicalizationPolicy") != POLICY_NAME:
        raise DomainError(f"ledger entry for {stream} names an unreviewed policy")
    published = row.get("sourcePublishedAt")
    if published is not None and (not isinstance(published, str) or not published):
        raise DomainError(f"ledger entry for {stream} has an invalid sourcePublishedAt")


def _source_artifact_thing(stream: str, row: dict[str, object]) -> dict[str, object]:
    data: dict[str, object] = {
        "source": SOURCE_WREF,
        "semanticSha256": row["semanticSha256"],
        "mediaType": MEDIA_TYPE,
        "byteLength": row["byteLength"],
        "originalUrl": row["originalUrl"],
        "durableUri": row["durableUri"],
        "acceptedAt": row["acceptedAt"],
        "canonicalizationPolicy": POLICY_NAME,
    }
    if row.get("sourcePublishedAt"):
        data["sourcePublishedAt"] = row["sourcePublishedAt"]
    return _thing("SourceArtifact", source_artifact_wref(stream), data)


def _build_games(schedules: list[dict[str, object]]) -> list[dict[str, object]]:
    artifact = source_artifact_wref(f"schedules/{SEASON}")
    games = []
    for row in schedules:
        wref = game_wref(row.get("game_id"))
        games.append(
            _thing(
                "Game",
                wref,
                {
                    "gameId": row["game_id"],
                    "season": _require_season(row.get("season")),
                    "week": _require_week(row.get("week")),
                    "seasonType": _season_type(row.get("game_type")),
                    "gameday": _string(row.get("gameday")),
                    "homeTeam": team_abbr(row.get("home_team")),
                    "awayTeam": team_abbr(row.get("away_team")),
                    "homeScore": row.get("home_score"),
                    "awayScore": row.get("away_score"),
                    "oldGameId": _string(row.get("old_game_id")),
                    "pfrId": _string(row.get("pfr")),
                    "espnId": _string(row.get("espn")),
                    "sourceArtifactWref": artifact,
                },
            )
        )
    return games


def _build_players(
    players: list[dict[str, object]], ff_playerids: list[dict[str, object]]
) -> list[dict[str, object]]:
    # Ported crosswalk dedupe: one ff_playerids row per gsis_id, keeping the
    # most recent db_season. The July SQL used ROW_NUMBER over db_season DESC
    # NULLS LAST, which left ties unordered; ties are now broken
    # deterministically by the highest numeric mfl_id (11 tied gsis_ids
    # profiled 2026-09-10).
    crosswalk: dict[str, tuple[tuple[int, int], dict[str, object]]] = {}
    for row in ff_playerids:
        gsis = row.get("gsis_id")
        if not isinstance(gsis, str) or not gsis:
            continue  # crosswalk rows without a gsis id cannot join the master list
        db_season = row.get("db_season")
        if db_season is None:
            db_season = -1
        if isinstance(db_season, bool) or not isinstance(db_season, int):
            raise DomainError(f"ff_playerids db_season is not an integer: {db_season!r}")
        rank = (db_season, int(str(row["mfl_id"])))
        current = crosswalk.get(gsis)
        if current is None or rank > current[0]:
            crosswalk[gsis] = (rank, row)

    source_artifact = source_artifact_wref("players")
    crosswalk_artifact = source_artifact_wref("ff-playerids")
    things = []
    for row in players:
        gsis = row.get("gsis_id")
        wref = player_wref(gsis)
        display_name = row.get("display_name")
        if not isinstance(display_name, str) or not display_name.strip():
            raise DomainError(f"player {gsis!r} lacks a display name")
        ff = crosswalk.get(gsis, (None, {}))[1]
        things.append(
            _thing(
                "Player",
                wref,
                {
                    "gsisId": gsis,
                    "displayName": display_name,
                    "position": _string(row.get("position")),
                    "positionGroup": _string(row.get("position_group")),
                    "pfrId": _string(row.get("pfr_id")),
                    "espnId": _string(row.get("espn_id")),
                    "sleeperId": _string(ff.get("sleeper_id")),
                    "yahooId": _string(ff.get("yahoo_id")),
                    "cbsId": _string(ff.get("cbs_id")),
                    "fantasyprosId": _string(ff.get("fantasypros_id")),
                    "mflId": _string(ff.get("mfl_id")),
                    "sportradarId": _string(ff.get("sportradar_id")),
                    "birthdate": _string(row.get("birth_date")),
                    "college": _string(row.get("college_name")),
                    "sourceArtifactWref": source_artifact,
                    "crosswalkArtifactWref": crosswalk_artifact,
                },
            )
        )
    return things


def _game_lookup(schedules: list[dict[str, object]]) -> dict[tuple[int, int, str], str]:
    # Ported: stats rows join to schedules on (season, week, team in
    # {home, away}) because player_stats.game_id is historically ~half null.
    lookup: dict[tuple[int, int, str], str] = {}
    for row in schedules:
        game_id = _require_id(row.get("game_id"), _GAME_ID, "game_id")
        season = _require_season(row.get("season"))
        week = _require_week(row.get("week"))
        for side in ("home_team", "away_team"):
            key = (season, week, team_abbr(row.get(side)))
            if key in lookup:
                raise DomainError(f"ambiguous schedule join key: {key}")
            lookup[key] = game_id
    return lookup


def _stat_fields(
    row: dict[str, object], int_map: dict[str, str], float_map: dict[str, str]
) -> dict[str, object]:
    data: dict[str, object] = {}
    for column, field in int_map.items():
        value = _zero_int(row.get(column))
        if value is not None:
            data[field] = value
    for column, field in float_map.items():
        value = _float(row.get(column))
        if value is not None and value != 0.0:
            data[field] = value
    return data


def _joined_game(
    lookup: dict[tuple[int, int, str], str], season: int, week: int, team: str, label: str
) -> str:
    game_id = lookup.get((season, week, team))
    if game_id is None:
        # The 2026 schedule must explain every 2026 stat row; a missing join is
        # malformed input, and malformed input fails the run (it never
        # authorizes skipping real stats).
        raise DomainError(f"no scheduled game for {label} ({season}, week {week}, {team})")
    return game_id


def _build_player_game_stats(
    rows: list[dict[str, object]], lookup: dict[tuple[int, int, str], str]
) -> list[dict[str, object]]:
    artifact = source_artifact_wref(f"player-stats/{SEASON}")
    things = []
    for row in rows:
        gsis = row.get("player_id")
        season = _require_season(row.get("season"))
        week = _require_week(row.get("week"))
        team = team_abbr(row.get("team"))
        wref = player_game_stats_wref(season, week, gsis)
        game_id = _joined_game(lookup, season, week, team, f"player {gsis}")
        data: dict[str, object] = {
            "playerWref": player_wref(gsis),
            "gameWref": game_wref(game_id),
            "teamWref": f"Team/{team}",
            "sourceArtifactWref": artifact,
            "season": season,
            "week": week,
            "seasonType": _season_type(row.get("season_type")),
            "opponentTeam": team_abbr(row.get("opponent_team")),
        }
        data.update(_stat_fields(row, PGS_INT, PGS_FLOAT))
        things.append(_thing("PlayerGameStats", wref, data))
    return things


def _build_team_game_stats(
    rows: list[dict[str, object]], lookup: dict[tuple[int, int, str], str]
) -> list[dict[str, object]]:
    artifact = source_artifact_wref(f"team-stats/{SEASON}")
    things = []
    for row in rows:
        team = team_abbr(row.get("team"))
        season = _require_season(row.get("season"))
        week = _require_week(row.get("week"))
        wref = team_game_stats_wref(season, week, team)
        game_id = _joined_game(lookup, season, week, team, f"team {team}")
        data: dict[str, object] = {
            "teamWref": f"Team/{team}",
            "gameWref": game_wref(game_id),
            "sourceArtifactWref": artifact,
            "season": season,
            "week": week,
            "seasonType": _season_type(row.get("season_type")),
            "opponentTeam": team_abbr(row.get("opponent_team")),
        }
        data.update(_stat_fields(row, TGS_INT, TGS_FLOAT))
        things.append(_thing("TeamGameStats", wref, data))
    return things
