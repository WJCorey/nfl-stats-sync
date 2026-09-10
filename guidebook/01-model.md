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

Desired Things have this shape:

```json
{
  "kind": "thing",
  "shape": "Person",
  "wref": "Person/source/alice",
  "data": { "name": "Alice" }
}
```

Collections name a relationship and contain endpoint references:

```json
{
  "kind": "collection",
  "shape": "Arc",
  "wref": "Arc/source/alice-team",
  "collectionType": "arc",
  "members": ["Person/source/alice", "Team/source/data"]
}
```

Assertions describe the Thing or relationship named by `about`:

```json
{
  "kind": "assertion",
  "shape": "Membership",
  "wref": "Membership/source/alice-team",
  "about": "Arc/source/alice-team",
  "data": { "role": "member" }
}
```

Desired state never contains operation verbs, versions, or active flags. Names are stable and unpinned. A Shape must match the first segment of its record Wref.

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

The project records the exact Shapes its output requires. Mushroom v1 does not create, synchronize, or guard those Shapes. Target-schema drift is a documented limitation: verify prerequisites manually before a live evaluation, and treat schema-related validation failures as a reason to stop rather than improvise.
