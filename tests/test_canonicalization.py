"""Permutation and rejection tests for agentgm-nflverse-canonical-jsonl/v1."""

from __future__ import annotations

import datetime
import json
import random

import pytest

from project.canonicalization import (
    SEASON,
    CanonicalizationError,
    canonical_value,
    canonicalize,
)

STREAM = f"team-stats/{SEASON}"


def _rows() -> list[dict[str, object]]:
    return [
        {
            "team": "KC", "season": SEASON, "week": 1, "season_type": "REG",
            "opponent_team": "LAC", "passing_yards": 258, "def_sacks": 1.5,
            "passing_epa": 0.123456789, "notes": None,
        },
        {
            "team": "LAC", "season": SEASON, "week": 1, "season_type": "REG",
            "opponent_team": "KC", "passing_yards": 201, "def_sacks": 2.0,
            "passing_epa": float("nan"),
        },
        {
            "team": "KC", "season": SEASON, "week": 2, "season_type": "REG",
            "opponent_team": "DEN", "passing_yards": 0, "def_sacks": 0.0,
        },
    ]


def test_shuffled_row_order_produces_identical_bytes_and_hash() -> None:
    baseline = canonicalize(STREAM, _rows())
    for seed in range(5):
        shuffled = _rows()
        random.Random(seed).shuffle(shuffled)
        permuted = canonicalize(STREAM, shuffled)
        assert permuted.payload == baseline.payload
        assert permuted.sha256 == baseline.sha256


def test_column_order_and_packaging_do_not_change_the_hash() -> None:
    baseline = canonicalize(STREAM, _rows())
    reordered = [dict(reversed(list(row.items()))) for row in _rows()]
    assert canonicalize(STREAM, reordered).sha256 == baseline.sha256
    # Equivalent transport packaging: the same claims arriving as separately
    # parsed pages/chunks canonicalize identically.
    pages = [_rows()[:1], _rows()[1:]]
    repackaged = [row for page in pages for row in page]
    assert canonicalize(STREAM, repackaged).sha256 == baseline.sha256
    # And a JSON round-trip of the canonical records reproduces the same bytes.
    roundtrip = [json.loads(json.dumps(record)) for record in baseline.records]
    assert canonicalize(STREAM, roundtrip).payload == baseline.payload


def test_any_changed_claim_value_changes_the_hash() -> None:
    baseline = canonicalize(STREAM, _rows())
    for column, value in (("passing_yards", 259), ("def_sacks", 1.6), ("opponent_team", "DEN")):
        rows = _rows()
        rows[0][column] = value
        assert canonicalize(STREAM, rows).sha256 != baseline.sha256


def test_nan_and_null_mean_no_value_and_integral_floats_are_integers() -> None:
    baseline = canonicalize(STREAM, _rows())
    rows = _rows()
    del rows[1]["passing_epa"]  # NaN and absent are the same "no value" claim
    del rows[0]["notes"]
    assert canonicalize(STREAM, rows).sha256 == baseline.sha256
    as_floats = _rows()
    as_floats[0]["passing_yards"] = 258.0  # parquet type flapping must not change bytes
    assert canonicalize(STREAM, as_floats).sha256 == baseline.sha256
    assert b'"def_sacks":2,' in canonicalize(STREAM, _rows()).payload  # 2.0 -> 2


def test_non_finite_and_unsupported_values_fail_closed() -> None:
    rows = _rows()
    rows[0]["passing_epa"] = float("inf")
    with pytest.raises(CanonicalizationError, match="non-finite"):
        canonicalize(STREAM, rows)
    with pytest.raises(CanonicalizationError, match="unsupported claim value"):
        canonical_value(object())


def test_dates_canonicalize_to_iso_strings() -> None:
    assert canonical_value(datetime.date(2026, 9, 13)) == "2026-09-13"
    assert canonical_value("2026-09-13") == "2026-09-13"


@pytest.mark.parametrize("bad_team", [None, ""])
def test_null_or_empty_key_columns_are_rejected(bad_team: object) -> None:
    rows = _rows()
    rows[0]["team"] = bad_team
    with pytest.raises(CanonicalizationError, match="null/empty key column"):
        canonicalize(STREAM, rows)


def test_duplicate_source_grain_keys_are_rejected() -> None:
    rows = _rows() + [_rows()[0]]
    with pytest.raises(CanonicalizationError, match="duplicate source-grain key"):
        canonicalize(STREAM, rows)


def test_rows_outside_the_declared_season_filter_are_rejected() -> None:
    rows = _rows()
    rows[0]["season"] = SEASON - 1
    with pytest.raises(CanonicalizationError, match="season filter"):
        canonicalize(STREAM, rows)


def test_unknown_streams_are_rejected() -> None:
    with pytest.raises(CanonicalizationError, match="unknown stream"):
        canonicalize("mystery-stream", [])


def test_player_stats_rows_without_player_id_are_excluded_and_counted() -> None:
    stream = f"player-stats/{SEASON}"
    rows = [
        {"player_id": "00-0033873", "season": SEASON, "week": 1, "team": "KC", "x": 1},
        {"player_id": None, "season": SEASON, "week": 1, "team": "KC", "x": 2},
        {"player_id": "", "season": SEASON, "week": 1, "team": "LAC", "x": 3},
    ]
    canonical = canonicalize(stream, rows)
    assert canonical.excluded_rows == 2
    assert len(canonical.records) == 1


def test_ff_playerids_sort_by_numeric_mfl_id() -> None:
    rows = [
        {"mfl_id": "10001", "name": "B"},
        {"mfl_id": "999", "name": "A"},
    ]
    canonical = canonicalize("ff-playerids", rows)
    assert [record["mfl_id"] for record in canonical.records] == ["999", "10001"]
    rows[0]["mfl_id"] = "abc"
    with pytest.raises(CanonicalizationError, match="not numeric"):
        canonicalize("ff-playerids", rows)
