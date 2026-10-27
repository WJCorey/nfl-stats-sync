# The Mushroom model

Mushroom keeps one boundary simple:

```text
frozen source ── project transform ── desired state
                                      + current WarmHub state
                                      └─ checked operations.jsonl
                                           ├─ server validation
                                           ├─ one submission
                                           └─ fresh readback + residual plan
```

Dagster orchestrates and reports this sequence. DuckDB compares complete files. WarmHub owns durable semantic identity. The whole operation plan is one asset; individual WarmHub records are not Dagster assets.

## Choose the synchronization unit

Ask what one successful synchronization claims is correct:

- If it replaces one complete current answer, use Mushroom's shipped unpartitioned flow. Each run updates the same managed set.
- If each release, season, week, or similar slice remains independently meaningful, it is a candidate semantic partition. A correction rematerializes the same stable partition key.

Do not partition merely for run history, performance, row chunks, or WarmHub request batches.

Mushroom's jobs are unpartitioned. They support complete-current synchronization and preserve-only accumulation. Independently running or backfilling slices, or retracting records within an old slice without an honest whole-scope complete snapshot, requires the proposed [semantic-partition pattern](07-semantic-partitions.md).

## Project-owned decisions

The project declares one target, one managed scope, one completeness claim, and one absence policy. It transforms a frozen snapshot into operation-neutral records.

Every desired record is a Thing with an independent name, explicit declared Shapes, and a body:

```json
{
  "name": "source/alice",
  "shapes": ["Person", "Researcher"],
  "data": { "displayName": "Alice", "field": "ecology" }
}
```

Collections and assertions are Things too. Their semantics live in the declared Shape contracts and ordinary body fields:

```json
{
  "name": "source/alice-team",
  "shapes": ["Arc"],
  "data": { "from": "source/alice", "to": "source/data-team" }
}
```

```json
{
  "name": "source/alice-membership",
  "shapes": ["Membership"],
  "data": { "about": "source/alice-team", "role": "member" }
}
```

A List uses `data.items`; a Set uses `data.members`. Other collection Shapes use their approved body contracts instead of a separate collection record kind. Desired state never contains operation verbs, versions, or active flags. Names are stable and unpinned; a Shape name does not determine a Thing's name. Current records use the same representation plus `active` and a positive `version`.

`shapes` contains actual declarations, not inherited certificates. Declaration order is insignificant. Current-state capture resolves reference fields through effective certified contracts, including inherited fields, and retains stored version selectors. For a versioned field, an unpinned desired reference to the same Thing preserves its current pin during an unrelated revision; an explicitly different selector remains a requested change. For an `either` field, an unpinned reference requests identity binding, so changing between pinned and unpinned forms is meaningful. Certified reference schemas travel with current-state evidence so inherited fields obey these rules. Reference Lists preserve their authored order and repeated members; versioned members retain history by identity through insertion and reordering. Arrays of objects still pair by position because no item identity contract is defined. Current capture retains retracted name holders for reactivation checks and submission identity, choosing the active holder or latest tombstone and rejecting ambiguous history. Ordinary strings are compared exactly.

## Whole-Thing ownership and declaration migrations

The managed name namespace grants authority over every non-Shape Thing matching the glob, regardless of its Shapes: its complete body and declaration set. Never combine `retract` with a namespace another writer uses. Selecting a Thing because it carries a particular Shape does not establish ownership. Independent writers may own disjoint names in one repository; independently maintaining fields or declarations on the same Thing is unsupported.

By default, `ALLOW_SHAPE_CHANGES=False` stops a plan when an existing Thing's desired declaration set changes. New Things receive their desired declarations. For an explicit migration, record the reviewed before/after declarations and body changes in `project/SPEC.md`, set `ALLOW_SHAPE_CHANGES=True`, and inspect the fresh plan. This policy is captured in the plan. Return to `False` after the migration converges. Operator rewrites can add `CorrectedThing` or `RedactedThing` declarations; inspect the operator history and review the resulting transition rather than treating it as ordinary source drift or automatically stripping system markers.

Ordinary body revisions and migration revisions both submit the entire desired declaration set. Mushroom does not guess which declarations another writer meant to preserve.

## What happens when a record disappears?

The managed scope states which record names this project may change. State outside it is untouched.

Completeness is a statement about one frozen run, not about the source in the abstract:

- `complete=true` means the snapshot contains every managed record whose absence could matter.
- `complete=false` means the run is partial, windowed, append-only, current-only, or otherwise cannot explain every missing managed record.

Choose one absence policy:

- `preserve`: keep current records missing from desired state;
- `retract`: retract missing current records using their expected versions.

Mushroom rejects `retract` with incomplete coverage. Choose it only when this exact run covers the whole managed set. For a grouped product, the combined coverage is no stronger than its weakest required source. Until every required source proves complete coverage of the same managed boundary, use `complete=false` and `preserve`.

### Current limit: one rule per project

One Mushroom project has one managed scope, one completeness claim, and one absence policy. It cannot preserve one group of records while retracting another group in the same plan.

If different groups need different policies, use `preserve` for the whole product or split them into separate Mushroom projects with disjoint managed scopes. When separate projects share a WarmHub repository, do not run their live writes at the same time.

## Target Shapes

The project records the exact Shapes its output requires. Mushroom does not create, synchronize, or guard those Shapes. Target-schema drift is a documented limitation: verify prerequisites manually before a live evaluation, and treat schema-related validation failures as a reason to stop rather than improvise.
