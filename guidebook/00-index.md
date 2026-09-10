# Author a Mushroom project

Mushroom turns a frozen source snapshot into reviewed WarmHub state. You own what the source means; Mushroom owns deterministic planning and the guarded write loop.

It teaches Mushroom's safety loop and the small set of Dagster CLI commands that operate it. It does not try to teach Dagster internals.

## Words used in this guide

- A **Shape** is a WarmHub record type.
- A **Wref** is a stable record name.
- **Desired state** is the set of records the source says should exist.
- **Current state** is what WarmHub contains now.
- A **synchronization unit** is what one successful run claims is correct.
- A **semantic partition** is an independently meaningful slice maintained under one stable key.
- A **managed scope** is the set of record names this project may change.
- An **absence policy** decides whether a current record missing from desired state is kept or retracted.
- To **retract** a record is to remove it after the source proves it is gone.

## Read the guidebook

Follow these pages in order:

1. [`01-model.md`](01-model.md) — learn the boundary and safety model.
2. [`02-onboard-a-source.md`](02-onboard-a-source.md) — design, approve, and implement `project/`.
3. [`03-plan-review.md`](03-plan-review.md) — run the credential-free review and inspect its exact operations.
4. [`04-credentials.md`](04-credentials.md) — enable narrowly scoped live access.
5. [`05-operate.md`](05-operate.md) — prepare, validate, synchronize, inspect, and recover.

If the WarmHub repository already contains records, begin with [`06-adopt-an-existing-repo.md`](06-adopt-an-existing-repo.md), then return to this sequence.

For accumulating products whose releases or slices remain independently meaningful, also review the proposed [`07-semantic-partitions.md`](07-semantic-partitions.md) pattern.

## Before you begin

Complete the [README prerequisites](../README.md#prerequisites). The [recommended authoring skills](../README.md#recommended-authoring-skills) help with WarmHub design, ingestion, Dagster, and large commits. When available, use `design-warmhub-repo`, `plan-warmhub-ingestion`, and `dagster-expert`; add `wh-commit-design` for a plan that may need more than one WarmHub request. Record the skills or canonical guidance consulted in `project/SPEC.md`.

## Choose a path

### New knowledge product

Start with [`02-onboard-a-source.md`](02-onboard-a-source.md). It leads from source inspection and product questions through human approval, manual Shape provisioning, and implementation.

### Existing WarmHub repository

Start with [`06-adopt-an-existing-repo.md`](06-adopt-an-existing-repo.md). Begin preserve-only and treat unexpectedly large additions or revisions as an identity question.

## Who owns what

| Area | Who decides |
| --- | --- |
| Source access, parsing, record names, relationships, managed scope, completeness, and absence policy | Project author, with domain review |
| Libraries and Python structure inside `project/` | Project author |
| Project-specific Dagster schedules, sensors, and asset checks | Project author, in a clearly named module under `src/mushroom/defs/` |
| Comparing desired and current state, saving and validating the plan, submitting it, and checking the result | Mushroom; do not replace |

Python is authoritative. These instructions are not parsed configuration and there is no mapping language or acquisition framework.

Mushroom assumes the target Shapes, identities, and assertion subjects are approved. Mushroom never creates Shapes. Resolve and approve those decisions before implementation or provisioning.

## Definition of done

A project is ready for review when:

- inspected source data deterministically produces the expected desired state;
- its identities, relationships, managed scope, completeness, and absence policy have explicit human approval recorded in `project/SPEC.md`;
- malformed or partial source input fails without authorizing retractions;
- `prepare_sync` produces the expected complete plan summary;
- the run is available in a compatible existing Dagster UI or a local one, with its URL and run ID reported for inspection;
- `synchronize` validates each checked batch immediately before applying it; standalone `validate_sync` is an optional advisory check when the plan fits one request; and
- an approved synchronization reaches fresh readback with no residual operations.

Green tests establish mechanics, not domain meaning. A human must still review whether the resulting knowledge is true and usefully modeled.

Stop and ask for domain guidance before guessing an identity, relationship, coverage claim, absence meaning, or target Shape. `src/mushroom/defs/` is the current Dagster definition-discovery seam, so project-owned schedules, sensors, and checks may live in a clearly named module there. Keep them separate from `sync.py`; project automation must not change the synchronization kernel or definition loader.
