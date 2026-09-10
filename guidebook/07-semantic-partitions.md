# Semantic partitions

Status: proposed authoring pattern. Mushroom's shipped jobs are unpartitioned, so this page defines the intended compatibility boundary rather than commands that work through `project/` today.

Use this pattern when each release or slice remains independently meaningful after later data arrives. Examples include a named tariff schedule revision or an NFL season/week. A correction rematerializes the same stable partition key; a new independently meaningful slice receives a new key.

Do not use partitions merely to retain run history, improve performance, divide rows, or match WarmHub request batches. A complete current answer, such as current members of Congress, remains one unpartitioned synchronization.

## Project-owned evidence

For each semantic partition, project code defines and records:

- a stable, validated, source-derived partition key;
- one immutable accepted source manifest or artifact digest for each materialization;
- how a corrected publication maps back to the same key;
- the disjoint Wref namespace owned by the slice;
- whether the source is complete inside that slice; and
- the prior converged writer-owned Wrefs whose absence may authorize retraction.

Shared records that do not belong to one release, such as a source description or content-addressed source artifact, stay outside partition ownership.

## Required execution properties

A partition-aware path must retain Mushroom's existing safety boundary:

- partition-namespaced source, current, desired, plan, result, readback, and residual artifacts;
- bounded current reads for the project-supplied desired and prior-owned Wrefs;
- deterministic reconciliation and expected versions;
- validation and checked submission;
- fresh readback and zero-residual convergence; and
- serialization of live writes to the same WarmHub target.

The partition key selects logical work; it does not freeze source bytes, narrow reads, or authorize retractions by itself.

## Corrections and retractions

New slices normally add records and preserve older slices. A correction to an existing slice may revise stable Wrefs.

Retraction is narrower: a complete correction may retract an active Wref only when it appears in that slice's prior converged writer-owned set and is absent from the newly accepted complete source. Unknown or manually created Wrefs outside the approved desired and prior-owned sets remain untouched. Publish the next owned-Wref set only after checked submission, fresh readback, and zero residual operations succeed.

For a populated slice with no trusted prior owned-Wref set, bootstrap in `preserve` mode. Exact-read the desired Wrefs, converge them, and publish those converged desired Wrefs as the initial owned set. A verified repository checkpoint or stopped-writer whole-repository read may supply current-state input, but another current Wref may enter the owned set only when trusted prior write receipts or an explicitly reviewed exclusive namespace or crosswalk proves ownership. The bootstrap run cannot retract anything.

## Dagster operation

Use one stable semantic key per partition. Runtime-discovered release identifiers are natural dynamic partitions; time partitions fit only when the calendar interval defines the slice. A correction rematerializes the same key. Historical loads select the required partitions as a backfill.

Irregular new releases and corrections usually need a sensor or manual registration. A schedule fits predictable polling, but neither schedules nor Dagster partitions decide publisher completeness or correction meaning.

## Current boundary

Current Mushroom uses one unpartitioned asset graph, one scope and absence policy, fixed mutable artifact refs, and a whole-target current-state read. Project code also does not submit operations directly. This pattern therefore is not runnable through `project/` alone; it needs reviewed lifecycle wiring that preserves the execution properties above.

Keep the source-specific meaning in project code. Do not bypass Mushroom's checked plan, submission, recovery, and convergence boundary.

See the [#9087 design note](https://github.com/warmhub/warmhub-app/issues/9087#issuecomment-5332101144), [Dagster partitions](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets), and [Dagster backfills](https://docs.dagster.io/guides/build/partitions-and-backfills/backfilling-data) for the deeper design.
