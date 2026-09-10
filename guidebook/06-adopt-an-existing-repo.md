# Adopt an existing repository

Use this path when a repository already contains records from an earlier proof or manual process. Start without deletions; this is not a clean load.

1. Read the existing Shapes and a representative set of records. Choose a deliberately narrow managed scope and name what is inside and outside it in `project/SPEC.md`. Complete the human approval checkpoint from [`02-onboard-a-source.md`](02-onboard-a-source.md) before implementing the transform or changing the target.
2. Decide identity before mapping. If existing Wrefs encode the same durable source identifiers, keep that formula. If they were hand-invented or use a different key, write a reviewed crosswalk from existing Wref to source key. If neither is possible, stop or use a separate new scope. Do not mint a second name for an existing record.
3. Start with `complete=false` and `ABSENCE_POLICY="preserve"`.
4. Optionally rehearse with a local copy of the repository. Capture the intended managed state with Mushroom's reader, then replace `project/fixtures/current.jsonl`:

   ```yaml
   # live-read.yaml — uncommitted
   resources:
     warmhub_client:
       config:
         enabled: true
   ops:
     current_state:
       config:
         live: true
   ```

   ```bash
   export MUSHROOM_TARGET=organization/repository
   uv run dg launch --assets current_state --config live-read.yaml
   export MUSHROOM_HOME="${MUSHROOM_HOME:-.mushroom}"
   jq . "$(jq -r '.key' "$MUSHROOM_HOME/refs/current.json" | sed "s#^#$MUSHROOM_HOME/#")" \
     > project/fixtures/current.jsonl
   ```

   This needs only whole-target `repo:read` authority. The asset normalizes Thing, collection, and assertion rows into Mushroom's canonical format; record target, capture time, and artifact digest in `project/SPEC.md`. It is a review aid, not an authoritative snapshot once the target changes.

5. Run `prepare_fresh_sync` and inspect the persisted plan. On a populated target, unexpectedly large additions are a stop signal just as large revisions are: `adds + preserved` with no revisions/retractions can be silent identity duplication.
6. For a one-request plan, `validate_sync` creates a fresh live plan and asks WarmHub to evaluate it without writing. For larger plans, Mushroom v1 has no read-only live-plan job; a separately approved `synchronize` run is the first live path and validates each batch just before applying it.
7. Rehearse the first live plan on a throwaway target or have a second reviewer inspect the identity decision and operations. After fresh readback proves stable identities, decide separately whether a complete source may authorize retractions.

Do not treat a zero-residual preserve plan as proof that unrelated stale records are gone. It proves only that the declared desired state and managed current state agree under the chosen policy.
