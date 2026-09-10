#!/usr/bin/env python3
"""Regenerate the frozen project fixtures (source, desired, operations).

The source fixture is a hand-made miniature of the real acquisition envelope,
produced through the SAME ``build_snapshot`` code path as a live ``acquire()``
(fake capture, temporary ledger) so the frozen format can never drift from the
live format. ``current.jsonl`` is hand-maintained: a handful of real records
read back from prod (agentgm/nfl-stats) on 2026-09-10 -- see project/SPEC.md.

Run from the repository root: ``uv run python scripts/generate_fixtures.py``.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from mushroom.kernel import ScopePolicy, canonical_jsonl, reconcile  # noqa: E402
from project import domain  # noqa: E402
from project.acquisition import build_snapshot  # noqa: E402
from project.canonicalization import STREAMS  # noqa: E402

FIXTURES = ROOT / "project" / "fixtures"

FROZEN_ACCEPTED_AT = "2026-09-09T12:00:00Z"
FROZEN_PUBLISHED_AT = "2026-09-09T10:00:00Z"

SCHEDULE_ROWS = [
    {
        "game_id": "2026_01_KC_LAC", "season": 2026, "game_type": "REG", "week": 1,
        "gameday": "2026-09-13", "home_team": "LAC", "away_team": "KC",
        "home_score": 27, "away_score": 21, "old_game_id": "2026091300",
        "pfr": "202609130sdg", "espn": "401999001", "spread_line": -2.5,
    },
    {
        "game_id": "2026_02_KC_DEN", "season": 2026, "game_type": "REG", "week": 2,
        "gameday": "2026-09-20", "home_team": "KC", "away_team": "DEN",
        "home_score": None, "away_score": None, "old_game_id": "2026092000",
        "pfr": None, "espn": "401999002", "spread_line": None,
    },
]

PLAYER_ROWS = [
    {
        "gsis_id": "00-0033873", "display_name": "Patrick Mahomes", "position": "QB",
        "position_group": "QB", "pfr_id": "MahoPa00", "espn_id": "3139477",
        "birth_date": "1995-09-17", "college_name": "Texas Tech", "status": "ACT",
    },
    {
        "gsis_id": "00-0099999", "display_name": "Rook E. Wideout", "position": "WR",
        "position_group": "WR", "pfr_id": None, "espn_id": "4999999",
        "birth_date": "2003-04-01", "college_name": "State U", "status": "ACT",
    },
]

FF_ROWS = [
    {
        "mfl_id": "13116", "gsis_id": "00-0033873", "sleeper_id": "4046",
        "yahoo_id": "30123", "cbs_id": "2142052", "fantasypros_id": "16413",
        "sportradar_id": "11cad59d-90dd-449c-a839-dddaba4fe16c", "db_season": 2026,
    },
    # Duplicate gsis with an older db_season: the crosswalk dedupe must ignore it.
    {
        "mfl_id": "11111", "gsis_id": "00-0033873", "sleeper_id": "9999",
        "yahoo_id": "1", "db_season": 2025,
    },
    # No gsis id: stays in the canonical artifact (mfl-keyed) but cannot join.
    {"mfl_id": "17999", "gsis_id": None, "sleeper_id": "12000", "db_season": 2026},
]

PLAYER_STATS_ROWS = [
    {
        "player_id": "00-0033873", "season": 2026, "week": 1, "season_type": "REG",
        "team": "KC", "opponent_team": "LAC",
        "completions": 24, "attempts": 39, "passing_yards": 258, "passing_tds": 1,
        "passing_interceptions": 0, "sacks_suffered": 2, "sack_yards_lost": -9,
        "passing_air_yards": 271, "passing_yards_after_catch": 116,
        "passing_first_downs": 9, "carries": 6, "rushing_yards": 57, "rushing_tds": 1,
        "rushing_first_downs": 5, "fumble_recovery_own": 1, "fumble_recovery_yards_own": 1,
        "def_sacks": 0.0, "fantasy_points": 26.02, "fantasy_points_ppr": 26.02,
        "passing_epa": 12.345678,  # analytics: artifact-only, dropped by the transform
    },
    {
        "player_id": "00-0099999", "season": 2026, "week": 1, "season_type": "REG",
        "team": "LAC", "opponent_team": "KC",
        "receptions": 5, "targets": 7, "receiving_yards": 63, "receiving_tds": 0,
        "fantasy_points": 6.3, "fantasy_points_ppr": 11.3,
        "wopr": 0.4567,  # analytics: artifact-only, dropped by the transform
    },
    # Outside the declared grain (no player_id): excluded from the canonical
    # artifact deterministically, counted in the envelope's exclusions.
    {
        "player_id": None, "season": 2026, "week": 1, "season_type": "REG",
        "team": "KC", "opponent_team": "LAC", "penalties": 1, "penalty_yards": 5,
    },
]

TEAM_STATS_ROWS = [
    {
        "team": "KC", "season": 2026, "week": 1, "season_type": "REG",
        "opponent_team": "LAC", "completions": 24, "attempts": 39,
        "passing_yards": 258, "def_sacks": 1.5, "def_sack_yards": 11.0,
        "penalties": 5, "penalty_yards": 45,
    },
    {
        "team": "LAC", "season": 2026, "week": 1, "season_type": "REG",
        "opponent_team": "KC", "completions": 18, "attempts": 30,
        "passing_yards": 201, "def_sacks": 2.0, "def_sack_yards": 14.0,
        "penalties": 3, "penalty_yards": 25,
    },
]

FIXTURE_ROWS = {
    "schedules/2026": SCHEDULE_ROWS,
    "players": PLAYER_ROWS,
    "ff-playerids": FF_ROWS,
    "player-stats/2026": PLAYER_STATS_ROWS,
    "team-stats/2026": TEAM_STATS_ROWS,
}


def frozen_envelope() -> bytes:
    def capture(spec):
        rows = FIXTURE_ROWS[spec.stream]
        return [dict(row) for row in rows], (
            FROZEN_PUBLISHED_AT if spec.release_api is not None else None
        )

    with TemporaryDirectory() as scratch:
        return build_snapshot(
            capture=capture,
            ledger_path=Path(scratch) / "ledger.json",
            staged_dir=Path(scratch) / "staged",
            now=lambda: FROZEN_ACCEPTED_AT,
            require_published=False,
            head_ok=lambda url, length: True,
        )


def main() -> None:
    assert set(FIXTURE_ROWS) == set(STREAMS)
    envelope = frozen_envelope()
    pretty = json.dumps(json.loads(envelope), ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    (FIXTURES / "source.json").write_text(pretty, encoding="utf-8")

    desired = canonical_jsonl(domain.transform(envelope))
    (FIXTURES / "desired.jsonl").write_bytes(desired)

    with TemporaryDirectory() as scratch:
        desired_path = Path(scratch) / "desired.jsonl"
        operations_path = Path(scratch) / "operations.jsonl"
        desired_path.write_bytes(desired)
        result = reconcile(
            desired_path,
            FIXTURES / "current.jsonl",
            operations_path,
            ScopePolicy("agentgm/nfl-stats", domain.MANAGED_SCOPE, False, domain.ABSENCE_POLICY),
        )
        (FIXTURES / "operations.jsonl").write_bytes(operations_path.read_bytes())
    summary = result.summary
    print(
        f"fixtures written: adds={summary.add_count} revisions={summary.revise_count} "
        f"retractions={summary.retract_count} unchanged={summary.unchanged_count} "
        f"preserved={summary.preserved_count}"
    )


if __name__ == "__main__":
    main()
