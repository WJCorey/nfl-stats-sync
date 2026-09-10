"""Domain tests: identity authority, transform goldens, fail-closed behavior."""

from __future__ import annotations

import json
import random
from pathlib import Path

import pytest

from mushroom.kernel import ScopePolicy, canonical_jsonl, reconcile
from project import domain
from project.canonicalization import SEASON, canonicalize


def _envelope() -> dict:
    return json.loads(domain.source_fixture().read_bytes())


def _bytes(envelope: dict) -> bytes:
    return json.dumps(envelope, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode() + b"\n"


def _with_stream(envelope: dict, stream: str, rows: list[dict]) -> dict:
    """Replace one stream's rows and recompute its ledger identity."""

    canonical = canonicalize(stream, rows)
    envelope["streams"][stream] = [dict(record) for record in canonical.records]
    envelope["ledger"][stream] = {
        **envelope["ledger"][stream],
        "semanticSha256": canonical.sha256,
        "byteLength": canonical.byte_length,
        "recordCount": len(canonical.records),
    }
    return envelope


def test_target_uses_environment_with_a_harmless_default(monkeypatch) -> None:
    monkeypatch.delenv("MUSHROOM_TARGET", raising=False)
    assert domain.target() == "example/mushroom-starter"
    monkeypatch.setenv("MUSHROOM_TARGET", "agentgm/nfl-stats")
    assert domain.target() == "agentgm/nfl-stats"


# ---------------------------------------------------------------------------
# Naming authority
# ---------------------------------------------------------------------------


def test_identity_formulas_continue_the_existing_prod_names() -> None:
    assert domain.player_wref("00-0033873") == "Player/00-0033873"
    assert domain.player_wref("ABB498348") == "Player/ABB498348"  # legacy pre-GSIS ids
    assert domain.game_wref("2026_01_KC_LAC") == "Game/2026_01_KC_LAC"
    assert (
        domain.player_game_stats_wref(SEASON, 1, "00-0033873")
        == f"PlayerGameStats/{SEASON}/01/00-0033873"
    )
    assert domain.team_game_stats_wref(SEASON, 18, "KC") == f"TeamGameStats/{SEASON}/18/KC"
    assert domain.source_artifact_wref("players") == "SourceArtifact/players"


@pytest.mark.parametrize(
    "value",
    [None, "", " ", "00-123", "00-12345678", "0-0033873", "abc498348", "AB498348", "00-0033873 ", 33873],
)
def test_malformed_player_ids_are_rejected_never_coerced(value: object) -> None:
    with pytest.raises(domain.DomainError, match="invalid gsis_id"):
        domain.player_wref(value)


@pytest.mark.parametrize("value", [None, "", "2026_1_KC_LAC", "2026-01-KC-LAC", "KC_LAC", 2026])
def test_malformed_game_ids_are_rejected(value: object) -> None:
    with pytest.raises(domain.DomainError, match="invalid game_id"):
        domain.game_wref(value)


@pytest.mark.parametrize("week", [0, 23, None, "1", 1.0, True])
def test_weeks_outside_the_nfl_calendar_are_rejected(week: object) -> None:
    with pytest.raises(domain.DomainError, match="invalid week"):
        domain.player_game_stats_wref(SEASON, week, "00-0033873")


@pytest.mark.parametrize("season", [2025, 2027, None, "2026", True])
def test_seasons_outside_the_managed_scope_are_rejected(season: object) -> None:
    with pytest.raises(domain.DomainError, match="season outside managed scope"):
        domain.team_game_stats_wref(season, 1, "KC")


def test_records_outside_the_approved_scope_families_are_rejected() -> None:
    for wref in ("Team/KC", "StatSource/nflverse-2026.07.02", "Game/2025_01_KC_LAC",
                 f"PlayerGameStats/2025/01/00-0033873", "Content/readme", "Source/espn"):
        with pytest.raises(domain.DomainError, match="outside the approved managed scope"):
            domain._assert_project_scope(wref)
    for wref in ("Source/nflverse", "SourceArtifact/players", f"Game/{SEASON}_01_KC_LAC",
                 "Player/00-0033873", f"PlayerGameStats/{SEASON}/01/00-0033873"):
        domain._assert_project_scope(wref)


# ---------------------------------------------------------------------------
# Transform goldens and determinism
# ---------------------------------------------------------------------------


def test_transform_matches_the_frozen_desired_state() -> None:
    desired = canonical_jsonl(domain.transform(domain.source_fixture().read_bytes()))
    assert desired == (domain.source_fixture().parent / "desired.jsonl").read_bytes()


def test_transform_is_deterministic_under_stream_row_reordering() -> None:
    baseline = canonical_jsonl(domain.transform(_bytes(_envelope())))
    envelope = _envelope()
    for rows in envelope["streams"].values():
        random.Random(7).shuffle(rows)
    assert canonical_jsonl(domain.transform(_bytes(envelope))) == baseline


def test_zero_omission_and_analytics_drop_are_ported_exactly() -> None:
    records = {r["wref"]: r for r in domain.transform(domain.source_fixture().read_bytes())}
    mahomes = records[f"PlayerGameStats/{SEASON}/01/00-0033873"]["data"]
    assert "passingInterceptions" not in mahomes  # zero count stat omitted
    assert "defSacks" not in mahomes  # 0.0 float omitted
    assert mahomes["sackYardsLost"] == -9  # nonzero negatives kept
    assert mahomes["fantasyPoints"] == 26.02
    assert not any(key.endswith("Epa") or key in {"pacr", "racr", "wopr"} for key in mahomes)
    assert mahomes["sourceArtifactWref"] == f"SourceArtifact/player-stats/{SEASON}"
    assert "statSourceWref" not in mahomes  # new records carry grounding provenance only
    rookie = records[f"PlayerGameStats/{SEASON}/01/00-0099999"]["data"]
    assert "receivingTds" not in rookie and rookie["receptions"] == 5


def test_players_carry_both_grounding_edges_and_the_crosswalk_dedupe_holds() -> None:
    records = {r["wref"]: r for r in domain.transform(domain.source_fixture().read_bytes())}
    mahomes = records["Player/00-0033873"]["data"]
    assert mahomes["sourceArtifactWref"] == "SourceArtifact/players"
    assert mahomes["crosswalkArtifactWref"] == "SourceArtifact/ff-playerids"
    assert mahomes["sleeperId"] == "4046"  # db_season 2026 row wins over 2025 duplicate
    assert mahomes["mflId"] == "13116"


def test_source_artifact_things_derive_purely_from_the_ledger() -> None:
    envelope = _envelope()
    records = {r["wref"]: r for r in domain.transform(_bytes(envelope))}
    for stream, row in envelope["ledger"].items():
        data = records[f"SourceArtifact/{stream}"]["data"]
        assert data["semanticSha256"] == row["semanticSha256"]
        assert data["byteLength"] == row["byteLength"]
        assert data["durableUri"] == row["durableUri"]
        assert data["acceptedAt"] == row["acceptedAt"]
        assert data["source"] == "Source/nflverse"
        assert data["canonicalizationPolicy"] == "agentgm-nflverse-canonical-jsonl/v1"
        assert data["mediaType"] == "application/x-ndjson"


def test_absent_optional_stream_yields_no_artifact_and_no_records() -> None:
    envelope = _envelope()
    for stream in (f"player-stats/{SEASON}", f"team-stats/{SEASON}"):
        del envelope["streams"][stream]
        del envelope["ledger"][stream]
    records = domain.transform(_bytes(envelope))
    wrefs = {record["wref"] for record in records}
    assert not any(wref.startswith(("PlayerGameStats/", "TeamGameStats/")) for wref in wrefs)
    assert f"SourceArtifact/player-stats/{SEASON}" not in wrefs
    assert f"SourceArtifact/schedules/{SEASON}" in wrefs


# ---------------------------------------------------------------------------
# Fail-closed behavior
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "mutate",
    [
        lambda e: e.update(complete=True),
        lambda e: e.pop("ledger"),
        lambda e: e.pop("streams"),
        lambda e: e["ledger"].pop("players"),  # coverage disagreement
        lambda e: e["streams"].pop("players"),
        lambda e: e["ledger"].update({"mystery": e["ledger"]["players"]}),
        lambda e: e["ledger"]["players"].update(semanticSha256="0" * 64),  # hash mismatch
        lambda e: e["ledger"]["players"].update(canonicalizationPolicy="other/v9"),
        lambda e: e["ledger"]["players"].update(durableUri="http://insecure.example/x"),
        lambda e: e["ledger"]["players"].pop("acceptedAt"),
        lambda e: e["streams"]["players"][0].update(display_name=None),
    ],
)
def test_malformed_snapshots_fail_without_authorizing_anything(mutate) -> None:
    envelope = _envelope()
    mutate(envelope)
    with pytest.raises(ValueError):
        domain.transform(_bytes(envelope))


def test_required_streams_cannot_be_absent() -> None:
    envelope = _envelope()
    del envelope["streams"][f"schedules/{SEASON}"]
    del envelope["ledger"][f"schedules/{SEASON}"]
    with pytest.raises(domain.DomainError, match="required streams missing"):
        domain.transform(_bytes(envelope))


def test_a_stat_row_without_a_scheduled_game_fails_the_run() -> None:
    envelope = _envelope()
    rows = envelope["streams"][f"player-stats/{SEASON}"]
    rows[0]["week"] = 3  # no scheduled (season, week, team) match
    _with_stream(envelope, f"player-stats/{SEASON}", rows)
    with pytest.raises(domain.DomainError, match="no scheduled game"):
        domain.transform(_bytes(envelope))


def test_duplicate_final_identities_are_rejected() -> None:
    envelope = _envelope()
    rows = envelope["streams"][f"player-stats/{SEASON}"]
    moved = dict(rows[0])
    moved["team"], moved["opponent_team"] = "LAC", "KC"  # same player-week, second team
    _with_stream(envelope, f"player-stats/{SEASON}", rows + [moved])
    with pytest.raises(domain.DomainError, match="duplicate desired identity"):
        domain.transform(_bytes(envelope))


def test_malformed_ids_inside_streams_fail_the_run() -> None:
    envelope = _envelope()
    rows = envelope["streams"]["players"]
    rows[0]["gsis_id"] = "totally-bogus"
    _with_stream(envelope, "players", rows)
    with pytest.raises(domain.DomainError, match="invalid gsis_id"):
        domain.transform(_bytes(envelope))


# ---------------------------------------------------------------------------
# Planning against the frozen current state
# ---------------------------------------------------------------------------


def test_fixture_plan_preserves_out_of_scope_history(tmp_path: Path) -> None:
    desired_path, operations_path = tmp_path / "desired.jsonl", tmp_path / "operations.jsonl"
    desired_path.write_bytes(canonical_jsonl(domain.transform(domain.source_fixture().read_bytes())))

    result = reconcile(
        desired_path,
        domain.current_state_fixture(),
        operations_path,
        ScopePolicy("agentgm/nfl-stats", domain.MANAGED_SCOPE, False, domain.ABSENCE_POLICY),
    )

    assert operations_path.read_bytes() == (
        domain.source_fixture().parent / "operations.jsonl"
    ).read_bytes()
    summary = result.summary
    assert (
        summary.add_count,
        summary.revise_count,
        summary.retract_count,
        summary.unchanged_count,
        summary.preserved_count,
    ) == (12, 1, 0, 1, 2)
    operations = [json.loads(line) for line in operations_path.read_bytes().splitlines()]
    names = {op["name"] for op in operations}
    # The 2025 record and the legacy StatSource are preserved, never revised or
    # retracted, and no duplicate identities are minted for them.
    assert "PlayerGameStats/2025/01/00-0033873" not in names
    assert "StatSource/nflverse-2026.07.02" not in names
    assert not any(op["operation"] == "retract" for op in operations)
