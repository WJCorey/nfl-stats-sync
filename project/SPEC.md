# agentgm/nfl-stats — 2026 season Mushroom synchronization

Keeps the populated public WarmHub repository **`agentgm/nfl-stats`** (124,073
things, seasons 2021–2025, ingested by the approved one-shot pipeline in the
agentgm workspace `ingest/`) fresh during the **2026 NFL season** from nflverse
data, with grounding 4.0 provenance (`Source`/`SourceArtifact`). This project
follows the guidebook's **adopt-an-existing-repo** path (`guidebook/06`):
preserve-only, identity-first, no clean load.

Guidance consulted: `guidebook/00–07` (esp. 01 model, 02 onboarding steps, 03
plan review, 06 adoption, 07 semantic partitions — noted, not used; see
"Synchronization unit"), the grounding 4.0 component contract
(`warmhub-data/grounding` 4.0.0 manifest + ontology guidebook Appendix C), the
agentgm workspace `SHAPES.md` / `DATA-SOURCES.md`, and the ported ingest
(`ingest/build_jsonl.py`, `ingest/create_shapes.py`, `ingest/README.md`).

## Human approval checkpoint

The design decisions below were approved in the project brief (Corey,
2026-09-10) and are recorded here for the formal sign-off. Only the human
approver may complete this checkpoint. **Do not run any live write before it is
signed.**

- [x] Repository namespace, visibility, and purpose — `agentgm/nfl-stats`, public, CC-BY foundation facts (pre-existing)
- [x] Product questions — Q1/Q2 continuation for 2026 + grounded provenance (below)
- [x] Shape names, kinds, fields, descriptions, and optionality — pre-existing shapes verified read-only on prod 2026-09-10 (see "Target Shapes"); Mushroom creates none
- [x] Stable record-name formulas and the source of each identifier — ported verbatim (below)
- [x] Relationships — wref edge fields on Things only; no collections/assertions in desired state
- [x] Repo-design review — the repo design predates this project (SHAPES.md, OS-1 resolved); this project adds only the approved grounding integration
- [x] Synchronization unit, managed names, completeness, absence — one preserve-only accumulation unit; `complete=false`; `preserve`; never retract
- [x] Source grain, ambiguities, and accepted information loss — five streams inspected 2026-09-10 (below)

Approved by:

Approved on:

## Product questions

1. **Q1 (2026):** what were player X's / team Y's stats in 2026 week W? →
   `PlayerGameStats/2026/**`, `TeamGameStats/2026/**`, `Game/2026_*`.
2. **Q2:** canonical identity + cross-platform IDs for every player → `Player/**`
   (master list refresh, crosswalk included).
3. **Provenance:** which exact canonical source snapshot established each
   record's claims? → `Source/nflverse` + `SourceArtifact/**` with
   `sourceArtifactWref` pins on grounded records.

## Source inspection (verified 2026-09-10 with HEAD requests and profiling)

| Stream | Transport (originalUrl) | Grain | Coverage | Provenance | Failure behavior |
|---|---|---|---|---|---|
| `player-stats/2026` | `https://github.com/nflverse/nflverse-data/releases/download/stats_player/stats_player_week_2026.parquet` (150 cols; 67 rows incl. 1 null-player aggregate on 2026-09-10) | one player-week stat line, key `(season, week, player_id, team)` | current-season snapshot, grows weekly; **optionally absent until first publication** | GitHub release asset `updated_at` | 404/empty before first publication → bootstrap rule; absent after acceptance → fail; malformed → fail |
| `team-stats/2026` | `https://github.com/nflverse/nflverse-data/releases/download/stats_team/stats_team_week_2026.parquet` (138 cols) | one team-week aggregate, key `(season, week, team)` | same as player-stats | same | same |
| `schedules/2026` | `https://github.com/nflverse/nflverse-data/releases/download/schedules/games.parquet` (46 cols; 272 REG rows for 2026; POST rows appear later in season) | one game, key `game_id` | full-history table filtered to season 2026 | GitHub release asset `updated_at` | required — fail closed on 404/empty/malformed |
| `players` | `https://github.com/nflverse/nflverse-data/releases/download/players/players.parquet` (39 cols; 24,823 rows, 0 null gsis, 0 dup gsis) | one player, key `gsis_id` | complete master list (not season-scoped) | GitHub release asset `updated_at` | required — fail closed |
| `ff-playerids` | `https://github.com/dynastyprocess/data/raw/master/files/db_playerids.csv` (35 cols; 12,492 rows) — published by DynastyProcess via nflverse (`nflreadr::load_ff_playerids`); **no parquet release asset exists**, the CSV in the repo tree is the publication | one fantasy-platform crosswalk row, key `mfl_id` (non-null, unique; 4,485 rows have gsis_id = NA) | complete crosswalk | none retrievable (repo file, no release asset) → `sourcePublishedAt` omitted | required — fail closed |

Raw transport downloads live under the gitignored `.cache/` directory, are
never committed, and must be deleted within 30 days (grounding contract);
every run re-downloads, so the cache can be cleared at any time.

License: all five streams are the plain CC-BY-4.0 nflverse core
(DATA-SOURCES.md licensing matrix). Attribution ships on `StatSource` (legacy)
and the repository README; no CC-BY-SA or proprietary tables are touched.

## Target Shapes (verified read-only on prod, 2026-09-10)

`Player@v2`, `Team@v1`, `Game@v2`, `PlayerGameStats@v2`, `TeamGameStats@v3`,
`StatSource@v1`, plus grounding 4.0 component shapes `Source@v1`,
`SourceArtifact@v1` (componentRef `warmhub-data/grounding`), and
`Source/nflverse@v1` exists. The current shape versions add optional
`sourceArtifactWref?` (wref → SourceArtifact) on
PlayerGameStats/TeamGameStats/Game/Player and `crosswalkArtifactWref?` (wref →
SourceArtifact) on Player. Mushroom never creates or revises Shapes.

> **⚠️ Pre-live blocker (operator):** `statSourceWref` is still declared
> **required** (no `?`) on `PlayerGameStats@v2` and `TeamGameStats@v3` per
> read-only `wh shape view`, while this project's approved contract is that new
> records emit `sourceArtifactWref` **instead of** `statSourceWref`. If the
> server enforces required fields at commit time, live validation of new
> 2026 stat records will fail until an operator revises those shapes to make
> `statSourceWref` optional (additive, no data migration). Verify with
> `validate_sync` or the first checked batch before enabling any schedule.

## Identity (naming authority: `project/domain.py`)

Formulas continue the existing prod identities verbatim (continuity spot-check:
10 of 10 names regenerated from the 2025 parquet resolve on prod, 2026-09-10):

| Record | Formula | Identifier source & validation |
|---|---|---|
| Player | `Player/<gsis_id>` | `players.gsis_id`; `^00-[0-9]{7}$` or legacy `^[A-Z]{3}[0-9]{6}$` (both profiled in the master list and already live on prod, e.g. `Player/ABB498348`) |
| Game | `Game/<game_id>` | `schedules.game_id`; `^\d{4}_\d{2}_[A-Z]{2,3}_[A-Z]{2,3}$` |
| PlayerGameStats | `PlayerGameStats/<season>/<week zero-padded 2>/<gsis_id>` | stats row axis; season must equal 2026, week 1–22 |
| TeamGameStats | `TeamGameStats/<season>/<week2>/<team_abbr>` | stats row axis; abbr `^[A-Z]{2,3}$` |
| Source | `Source/nflverse` (constant) | — |
| SourceArtifact | `SourceArtifact/<stream>` where stream ∈ {`player-stats/2026`, `team-stats/2026`, `schedules/2026`, `players`, `ff-playerids`} | stream identity = the logical dataset stream; versions are accepted snapshots (never hash/date/run-id in the name) |

Identity code rejects missing/empty/malformed identifiers, never coerces
`None`, is order-independent, and rejects duplicate final names. Known
identity limitation: a player credited with stats for **two teams in the same
week** would collide at `PlayerGameStats/<season>/<week2>/<gsis>`; the run
fails on the duplicate rather than guessing (no such row exists in 2021–2025
data; stop for domain guidance if it ever fires).

## Synchronization unit, managed scope, completeness, absence

- **Unit:** one preserve-only accumulation over the whole managed scope. NFL
  season/week slices are natural semantic partitions (guidebook 07), but that
  pattern is not runnable through `project/` today; since this project only
  adds and revises (never retracts), the shipped unpartitioned flow is
  sufficient and safe.
- **Semantic managed scope (six families):** `Game/2026_*`,
  `PlayerGameStats/2026/**`, `TeamGameStats/2026/**`, `Player/**`,
  `Source/nflverse`, `SourceArtifact/**`. NOT in scope: `Team/*`,
  `StatSource/*`, all 2021–2025 records, `ontology/*` records, `Content/*`,
  license/component records — never emitted, never touched.
- **Kernel scope limitation:** Mushroom accepts exactly one scope glob and its
  glob grammar cannot express the six-family union, so
  `MANAGED_SCOPE = "*/**"` at the kernel and the six families are enforced on
  every desired record by `domain._assert_project_scope` (tested). Under
  `preserve` the wide kernel scope cannot authorize any retraction; its only
  live effect is that `current_state`/`readback` read the whole repository
  (~124k records) rather than a subset. Revisit if Mushroom grows multi-glob
  scopes.
- **Completeness:** `complete=false` on every snapshot (the season is still
  being played; the master lists shed rows over time).
- **Absence policy:** `preserve`. **Never emit retractions.** Records that
  leave the master lists (e.g. ~200 players removed from `players.parquet`
  since the July pull) remain on prod untouched.
- **Desired-state kinds:** things only — no collections, no assertions.

## Transform decisions (ported exactly from `ingest/build_jsonl.py`)

- **Zero-omission convention:** zero-valued count stats are omitted (absence
  == 0 for a count); float stats omitted when 0.0; floats rounded to 2dp;
  `None`/empty-string values dropped from payloads.
- **Analytics drop (desired state only):** `passing_epa`, `passing_cpoe`,
  `pacr`, `rushing_epa`, `receiving_epa`, `racr`, `target_share`,
  `air_yards_share`, `wopr` are not promoted. They **stay in the canonical
  artifacts** — dropping is a transform decision, not artifact
  canonicalization.
- **seasonType:** `REG` if `game_type`/`season_type` == "REG" else `POST`.
- **gameWref join:** stats rows join schedules on `(season, week, team ∈
  {home, away})` (player_stats.game_id is historically ~half null). A 2026
  stat row with no scheduled match **fails the run** — deviation from
  build_jsonl, which warned-and-skipped; a full-season schedule must explain
  every stat row, and malformed input fails without authorizing anything.
- **Null-player_id rows** (team/unattributed aggregates) are outside the
  declared player-stats grain: excluded deterministically at canonicalization,
  counted, and reported in the snapshot envelope (`exclusions`); fail-open is
  not allowed for any other malformation.
- **Player crosswalk dedupe:** one `ff_playerids` row per `gsis_id`, keeping
  max `db_season`. Determinism refinement over the July `ROW_NUMBER()` SQL:
  ties (11 gsis_ids profiled) break by highest numeric `mfl_id`.
- **Field mappings:** camelCase maps `PGS_INT`/`PGS_FLOAT`/`TGS_INT`/
  `TGS_FLOAT` and the Player/Game builders are ported verbatim (see
  `project/domain.py`).
- **Grounding stamps (new versus the July ingest):** new/refreshed records
  carry `sourceArtifactWref` with the **bare stream name** (WarmHub pins the
  version at commit): stats → their stream artifact, Game →
  `SourceArtifact/schedules/2026`, Player → `SourceArtifact/players` +
  `crosswalkArtifactWref: SourceArtifact/ff-playerids`. New records do **not**
  emit `statSourceWref` and no new `StatSource` things are created; legacy
  records keep theirs untouched.
- **Season scope:** `SEASONS = (2026,)` — only 2026 rows enter desired state,
  plus all Players from the master lists (not season-scoped).

## Mapping classification (promoted / artifact-only / ignored)

**Promoted** (appear on desired Things):

- PlayerGameStats ← player-stats axis (`player_id`, `season`, `week`,
  `season_type`, `team`, `opponent_team`) + the 74 `PGS_INT` count columns +
  4 `PGS_FLOAT` columns (`def_sacks`, `def_sack_yards`, `fantasy_points`,
  `fantasy_points_ppr`), exactly as in `project/domain.py`.
- TeamGameStats ← team-stats axis + the same count columns + `def_sacks`,
  `def_sack_yards`.
- Game ← `game_id`, `season`, `week`, `game_type`→`seasonType`, `gameday`,
  `home_team`, `away_team`, `home_score`, `away_score`, `old_game_id`,
  `pfr`→`pfrId`, `espn`→`espnId`.
- Player ← players `gsis_id`, `display_name`, `position`, `position_group`,
  `pfr_id`, `espn_id`, `birth_date`→`birthdate`, `college_name`→`college`;
  ff-playerids `sleeper_id`, `yahoo_id`, `cbs_id`, `fantasypros_id`, `mfl_id`,
  `sportradar_id` (all id crosswalk values stringified).

**Artifact-only** (kept in the canonical artifacts, not promoted; the 9
analytics columns above plus):

- player-stats: `def_2pt_atts, def_2pt_made, def_fg_blocks, def_pat_blocks,
  def_punt_blocks, def_tackles_with_assist, fg_blocked_distance,
  fg_blocked_list, fg_made_distance, fg_made_list, fg_missed_distance,
  fg_missed_list, fg_pct, fumbles_forced_by_opp, fumbles_lost_total,
  fumbles_not_forced, fumbles_out_of_bounds, fumbles_total, game_id, gwfg_att,
  gwfg_blocked, gwfg_distance, gwfg_made, gwfg_missed, headshot_url,
  misc_yards, passing_10, passing_16, passing_20, passing_40, pat_pct,
  player_display_name, player_name, position, position_group, pt_att,
  pt_blocked, pt_downed, pt_fair_caught, pt_inside_20, pt_long, pt_net_yards,
  pt_out_of_bounds, pt_return_tds, pt_return_yards, pt_returned, pt_touchback,
  pt_yards, receiving_10, receiving_16, receiving_20, receiving_40,
  rushing_10, rushing_12, rushing_20, rushing_40` (new columns nflverse added
  after the July shape design; candidates for a future additive shape rev).
- team-stats: same families plus `timeouts`, minus the player-name/headshot
  columns.
- schedules: `away_coach, away_moneyline, away_qb_id, away_qb_name, away_rest,
  away_spread_odds, div_game, ftn, gametime, gsis, home_coach, home_moneyline,
  home_qb_id, home_qb_name, home_rest, home_spread_odds, location,
  nfl_detail_id, over_odds, overtime, pff, referee, result, roof, spread_line,
  stadium, stadium_id, surface, temp, total, total_line, under_odds, weekday,
  wind`.
- players: `college_conference, common_first_name, draft_pick, draft_round,
  draft_team, draft_year, esb_id, first_name, football_name, headshot, height,
  jersey_number, last_name, last_season, latest_team, nfl_id, ngs_position,
  ngs_position_group, ngs_status, ngs_status_short_description, otc_id,
  pff_id, pff_position, pff_status, rookie_season, short_name, smart_id,
  status, suffix, weight, years_of_experience`.
- ff-playerids: `age, birthdate, cfbref_id, college, db_season, draft_ovr,
  draft_pick, draft_round, draft_year, espn_id, fantasy_data_id,
  fleaflicker_id, height, ktc_id, merge_name, name, nfl_id, pff_id, pfr_id,
  position, rotowire_id, rotoworld_id, stats_global_id, stats_id, swish_id,
  team, twitter_username, weight` (`db_season` is also read by the dedupe
  rule; the crosswalk id columns listed under Promoted are read via the
  dedupe winner).

**Ignored** (transport, not claims): CSV missing-value markers (`''`/`'NA'`
decoded to null), parquet page/row-group structure, HTTP envelopes. Nothing
else is excluded — the canonical artifacts keep every source column.

Note on `headshot_url`/`headshot`: kept in the canonical artifact (it is a
source claim — a URL string, not an image); never promoted (NFL image rights,
per SHAPES.md).

## Canonicalization policy — `agentgm-nflverse-canonical-jsonl/v1`

Module: `project/canonicalization.py` (the reviewed stable contract named by
every `SourceArtifact.canonicalizationPolicy`). Per stream:

1. Parse all transport needed for complete stream coverage (single parquet or
   CSV file per stream).
2. Project ALL source columns; exclude nothing but transport. Decode R-style
   CSV missing values (`''`, `'NA'`) to null for `ff-playerids`.
3. Apply the declared season filter (season = 2026) for the three
   season-scoped streams — part of the declared stream grain.
4. Canonical values: nulls and float NaN omitted (both mean "no value" in
   nflverse outputs); non-finite numbers rejected; dates/timestamps → ISO-8601
   strings; integral floats within 2^53 → JSON integers (JCS integral form —
   int/float parquet-type flapping cannot change the hash); other floats via
   Python shortest-repr (ECMAScript-compatible for the doubles that occur
   here); strings verbatim.
5. Sort records by the declared stable source-grain key — player-stats:
   `(season, week, player_id, team)`; team-stats: `(season, week, team)`;
   schedules: `game_id`; players: `gsis_id`; ff-playerids: `mfl_id` compared
   numerically (DynastyProcess's primary key; profiled non-null and unique —
   gsis_id cannot key this stream because 4,485 rows lack it). Null/empty/
   duplicate keys are rejected, except the declared player-stats grain
   exemption: rows with null/empty `player_id` are outside the player-week
   grain (team aggregates), excluded deterministically and counted.
6. Serialize each record as RFC 8785/JCS-compatible canonical JSON (sorted
   keys, minimal separators, UTF-8), one record per LF line, final LF; SHA-256
   the bytes.

Permutation tests (`tests/test_canonicalization.py`) prove: shuffled row
order, column-order changes, and equivalent transport packaging produce
identical bytes+hash; any changed claim value changes the hash. Policy changes
require bumping to `/v2` and recording the migration here.

## Artifact ledger (`artifacts/ledger.json`, committed)

Maps stream → `{semanticSha256, byteLength, recordCount, mediaType,
acceptedAt, sourcePublishedAt?, originalUrl, durableUri,
canonicalizationPolicy}`. `acquire()` canonicalizes each stream and compares
hashes:

- **unchanged** → the ledger row is byte-for-byte untouched (`acceptedAt` is
  the first acceptance; unchanged recapture is a complete no-op — verified
  live 2026-09-10);
- **changed** → canonical bytes staged at
  `artifacts/staged/<stream-slug>/<sha256>.jsonl` (gitignored,
  content-addressed, regenerable), row rewritten with `acceptedAt` = now
  (UTC), `sourcePublishedAt` = GitHub release asset `updated_at` when
  retrievable, `durableUri` templated (default
  `https://github.com/warmautomation/nfl-stats-sync/releases/download/artifacts/<stream-slug>-<sha16>.jsonl`,
  override `NFLSTATS_DURABLE_URI_TEMPLATE`).

The transform derives `SourceArtifact` desired things **purely from the
ledger** (and re-canonicalizes the embedded streams to prove the snapshot is
coherent), and stamps grounded records with the bare stream name — WarmHub
pins the exact artifact version at commit, satisfying "every changed grounded
record pins the artifact version that established its claims" without the
producer tracking version numbers.

**Bootstrap rule:** `player-stats/2026` and `team-stats/2026` are declared
optionally-absent-until-first-publication. An absent upstream asset (HTTP 404,
or zero rows at the declared grain) yields **no artifact update and no desired
records for that stream** — not an empty artifact. Once a stream has a ledger
row, upstream disappearance fails the run. The other three streams fail closed
when unavailable, empty, or malformed. (As of 2026-09-10 all five streams are
live — week 1 has begun — so the bootstrap path is exercised by tests, not by
the current upstream state.)

**Durable-locator host decision — PENDING Corey's approval:** public GitHub
Releases on this repository (`warmautomation/nfl-stats-sync`, release tag
`artifacts`). `scripts/publish_artifacts.sh` uploads staged artifacts
idempotently. Live `synchronize` fails closed unless every ledger `durableUri`
answers an HTTP HEAD with the exact canonical byte length (enforced whenever
`WH_TOKEN` or `NFLSTATS_REQUIRE_PUBLISHED_ARTIFACTS` is set). Note: the target
repo is public but this locator host repo is currently **private** — readers
of `agentgm/nfl-stats` can only retrieve artifact bytes if the host repo (or
at least its `artifacts` release) is made public, which is part of the pending
decision.

## Frozen vectors and evaluation evidence

- `project/fixtures/source.json` — hand-made miniature snapshot generated
  through the real `build_snapshot` path (`scripts/generate_fixtures.py`;
  byte-equivalence enforced by `test_committed_fixture_matches_its_generator`).
- `project/fixtures/current.jsonl` — real prod records read back read-only on
  2026-09-10: `Source/nflverse@v1` (plans unchanged),
  `StatSource/nflverse-2026.07.02@v1` (preserved),
  `PlayerGameStats/2025/01/00-0033873@v1` (out-of-scope 2025 history,
  preserved), `Player/00-0033873@v1` (revises to gain the grounding wrefs).
- `project/fixtures/desired.jsonl`, `operations.jsonl` — frozen goldens.
- Fixture plan (prepare_sync, 2026-09-10, run `68817e2c`):
  adds=12, revisions=1, retractions=0, unchanged=1, preserved=2; operations
  sha256 `ef65d38b…8959`. The 2025 record and legacy StatSource appear in no
  operation.
- Fresh plan (prepare_fresh_sync against live nflverse, 2026-09-10, run
  `ab5f5227`): adds=25,167 (24,822 Player + 272 Game + 66 PlayerGameStats +
  2 TeamGameStats + 5 SourceArtifact), revisions=1 (Player/00-0033873 gains
  grounding wrefs), retractions=0, unchanged=1 (Source/nflverse),
  preserved=2. Note this plans against the small fixture current state; on the
  real populated target expect ~24.8k Player **revisions** instead of adds
  (every Player gains `sourceArtifactWref`/`crosswalkArtifactWref`) plus
  preserved ≈ 99k historical records — review that first live plan against
  guidebook 03's large-revision checklist before submitting.
- Identity continuity: 10/10 `PlayerGameStats/2025/…` names regenerated from
  the 2025 parquet resolve on prod via read-only `wh thing view`.
- Ledger no-op: second live `acquire()` left `artifacts/ledger.json`
  byte-identical (sha `352f527d…9523`).

## Operating runbook

1. `uv sync --frozen --group dev`; `uv run pytest`; `uv run dg check defs`.
2. `export DAGSTER_HOME=$PWD/.dagster MUSHROOM_TARGET=agentgm/nfl-stats` then
   `uv run dg launch --job prepare_fresh_sync`; review the saved plan
   (guidebook 03).
3. Commit `artifacts/ledger.json` when it changes; run
   `scripts/publish_artifacts.sh`; verify every `durableUri` resolves.
4. Optionally refresh `project/fixtures/current.jsonl` from prod with the
   `live-read.yaml` recipe in guidebook 06 (needs repo:read only).
5. After the approval checkpoint is signed and the `statSourceWref` shape
   blocker is resolved: supply `WARMHUB_API_URL`/`WH_TOKEN`/`MUSHROOM_TARGET`,
   create the uncommitted `live.yaml` (guidebook 04/05 — **never commit it**),
   `uv run dg launch --job validate_sync --config live.yaml` (fits one request
   only for small plans; the first full plan is large, so the first live path
   is a separately approved `synchronize`), then
   `uv run dg launch --job synchronize --config live.yaml`.
6. Automation (both stopped/gated by default; **single writer** — enable at
   most one): Dagster schedule `nflstats_daily_sync`
   (`src/mushroom/defs/nflstats_automation.py`, 06:30 America/New_York daily,
   STOPPED) or GitHub Actions (`.github/workflows/sync.yml`,
   workflow_dispatch; cron commented out; live job requires repo variable
   `LIVE_SYNC_APPROVED=true` plus secrets; `.mushroom/` retained 30 days).

## Open items for the operator

- [ ] Sign the human approval checkpoint above.
- [ ] Decide the durable-locator host (default: public GitHub Releases on this
  repo) and the visibility consequence for artifact readers; then publish the
  five staged artifacts and re-verify.
- [ ] Resolve the `statSourceWref` required-field conflict on
  `PlayerGameStats@v2` / `TeamGameStats@v3` (shape revise to optional, or an
  explicit decision that new records must keep emitting it).
- [ ] Provision live credentials (guidebook 04) and choose the single writer
  before enabling any schedule or the CI live job.
- [ ] Before the first live synchronize, refresh
  `project/fixtures/current.jsonl` from prod (guidebook 06 step 4) and review
  the resulting plan: expect ~24.8k Player revisions, 2026 adds, zero
  retractions, and a large preserved count.
