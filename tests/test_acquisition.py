"""Acquisition tests: ledger reconciliation, bootstrap rule, live fail-closed."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from project.acquisition import AcquisitionError, build_snapshot
from project.canonicalization import SEASON, STREAMS

PLAYER_STATS = f"player-stats/{SEASON}"
TEAM_STATS = f"team-stats/{SEASON}"
SCHEDULES = f"schedules/{SEASON}"

_ROWS: dict[str, list[dict[str, object]]] = {
    SCHEDULES: [
        {
            "game_id": "2026_01_KC_LAC", "season": SEASON, "game_type": "REG", "week": 1,
            "gameday": "2026-09-13", "home_team": "LAC", "away_team": "KC",
        }
    ],
    "players": [{"gsis_id": "00-0033873", "display_name": "Patrick Mahomes"}],
    "ff-playerids": [{"mfl_id": "13116", "gsis_id": "00-0033873", "db_season": 2026}],
    PLAYER_STATS: [
        {"player_id": "00-0033873", "season": SEASON, "week": 1, "season_type": "REG",
         "team": "KC", "opponent_team": "LAC", "passing_yards": 258}
    ],
    TEAM_STATS: [
        {"team": "KC", "season": SEASON, "week": 1, "season_type": "REG",
         "opponent_team": "LAC", "passing_yards": 258}
    ],
}


def _capture(rows=None, absent=()):
    rows = rows or _ROWS

    def capture(spec):
        if spec.stream in absent:
            return None
        return [dict(row) for row in rows[spec.stream]], "2026-09-10T00:00:00Z"

    return capture


def _snapshot(tmp_path: Path, **overrides) -> bytes:
    options = dict(
        capture=_capture(),
        ledger_path=tmp_path / "ledger.json",
        staged_dir=tmp_path / "staged",
        now=lambda: "2026-09-10T06:30:00Z",
        require_published=False,
        head_ok=lambda url, length: True,
    )
    options.update(overrides)
    return build_snapshot(**options)


def test_first_acquire_populates_ledger_and_stages_artifacts(tmp_path: Path) -> None:
    payload = _snapshot(tmp_path)
    envelope = json.loads(payload)
    assert envelope["complete"] is False
    assert set(envelope["ledger"]) == set(STREAMS)
    assert envelope["absentStreams"] == []
    ledger = json.loads((tmp_path / "ledger.json").read_bytes())
    for stream, row in ledger.items():
        staged = tmp_path / "staged" / stream.replace("/", "-") / f"{row['semanticSha256']}.jsonl"
        assert staged.exists()
        assert len(staged.read_bytes()) == row["byteLength"]
        assert row["acceptedAt"] == "2026-09-10T06:30:00Z"
        assert row["canonicalizationPolicy"] == "agentgm-nflverse-canonical-jsonl/v1"
        assert row["durableUri"].startswith("https://github.com/warmautomation/nfl-stats-sync/")


def test_unchanged_recapture_is_a_complete_ledger_noop(tmp_path: Path) -> None:
    first = _snapshot(tmp_path)
    ledger_bytes = (tmp_path / "ledger.json").read_bytes()
    staged = sorted(path for path in (tmp_path / "staged").rglob("*.jsonl"))

    second = _snapshot(tmp_path, now=lambda: "2026-09-11T06:30:00Z")

    assert second == first  # identical frozen snapshot bytes
    assert (tmp_path / "ledger.json").read_bytes() == ledger_bytes  # acceptedAt untouched
    assert sorted(path for path in (tmp_path / "staged").rglob("*.jsonl")) == staged


def test_changed_stream_updates_only_its_ledger_row(tmp_path: Path) -> None:
    _snapshot(tmp_path)
    before = json.loads((tmp_path / "ledger.json").read_bytes())
    rows = {stream: [dict(row) for row in stream_rows] for stream, stream_rows in _ROWS.items()}
    rows[PLAYER_STATS][0]["passing_yards"] = 300  # a stat correction lands upstream

    _snapshot(tmp_path, capture=_capture(rows), now=lambda: "2026-09-12T06:30:00Z")

    after = json.loads((tmp_path / "ledger.json").read_bytes())
    assert after[PLAYER_STATS]["semanticSha256"] != before[PLAYER_STATS]["semanticSha256"]
    assert after[PLAYER_STATS]["acceptedAt"] == "2026-09-12T06:30:00Z"
    for stream in STREAMS:
        if stream != PLAYER_STATS:
            assert after[stream] == before[stream]
    staged = tmp_path / "staged" / PLAYER_STATS.replace("/", "-")
    assert len(list(staged.glob("*.jsonl"))) == 2  # both accepted snapshots retained


def test_bootstrap_rule_optional_streams_absent_until_first_publication(tmp_path: Path) -> None:
    payload = _snapshot(tmp_path, capture=_capture(absent=(PLAYER_STATS, TEAM_STATS)))
    envelope = json.loads(payload)
    assert envelope["absentStreams"] == sorted([PLAYER_STATS, TEAM_STATS])
    assert PLAYER_STATS not in envelope["ledger"]
    assert PLAYER_STATS not in envelope["streams"]
    ledger = json.loads((tmp_path / "ledger.json").read_bytes())
    assert set(ledger) == set(STREAMS) - {PLAYER_STATS, TEAM_STATS}


def test_accepted_stream_vanishing_upstream_fails_closed(tmp_path: Path) -> None:
    _snapshot(tmp_path)
    with pytest.raises(AcquisitionError, match="now absent"):
        _snapshot(tmp_path, capture=_capture(absent=(PLAYER_STATS,)))


@pytest.mark.parametrize("stream", [SCHEDULES, "players", "ff-playerids"])
def test_required_streams_fail_closed_when_absent(tmp_path: Path, stream: str) -> None:
    with pytest.raises(AcquisitionError, match="required stream"):
        _snapshot(tmp_path, capture=_capture(absent=(stream,)))


def test_partial_acquisition_leaves_no_snapshot_to_plan_from(tmp_path: Path) -> None:
    calls = {"count": 0}

    def flaky(spec):
        calls["count"] += 1
        if calls["count"] > 2:
            raise AcquisitionError("connection reset mid-run")
        return _capture()(spec)

    with pytest.raises(AcquisitionError, match="connection reset"):
        _snapshot(tmp_path, capture=flaky)


def test_unknown_ledger_streams_require_a_deliberate_migration(tmp_path: Path) -> None:
    ledger_path = tmp_path / "ledger.json"
    ledger_path.write_text(json.dumps({"player-stats/2019": {"semanticSha256": "x"}}))
    with pytest.raises(AcquisitionError, match="no longer declares"):
        _snapshot(tmp_path)


def test_live_synchronization_requires_published_durable_uris(tmp_path: Path) -> None:
    _snapshot(tmp_path)
    with pytest.raises(AcquisitionError, match="scripts/publish_artifacts.sh"):
        _snapshot(tmp_path, require_published=True, head_ok=lambda url, length: False)
    # Once every durableUri answers a HEAD with the exact bytes, the run passes.
    checked: list[tuple[str, int]] = []

    def head_ok(url: str, length: int) -> bool:
        checked.append((url, length))
        return True

    _snapshot(tmp_path, require_published=True, head_ok=head_ok)
    assert len(checked) == len(STREAMS)


def test_committed_fixture_matches_its_generator() -> None:
    """The frozen source fixture must be regenerable byte-for-byte."""

    import importlib.util

    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "generate_fixtures", root / "scripts" / "generate_fixtures.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    committed = json.loads((root / "project" / "fixtures" / "source.json").read_bytes())
    assert json.loads(module.frozen_envelope()) == committed
