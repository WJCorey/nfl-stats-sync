# Adopt an existing repository

Use this path when a repository already contains records from an earlier proof or manual process. Start without deletions; this is not a clean load.

1. Read the existing Shapes and a representative set of records. Choose a deliberately narrow managed scope and name what is inside and outside it in `project/SPEC.md`. Complete the human approval checkpoint from [`02-onboard-a-source.md`](02-onboard-a-source.md) before implementing the transform or changing the target.
2. Decide identity before mapping. If existing names encode the same durable source identifiers, keep that formula. If they were hand-invented or use a different key, write a reviewed crosswalk from existing name to source key. If neither is possible, stop or use a separate new scope. Do not mint a second name for an existing record.
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
   jq -c . "$(jq -r '.key' "$MUSHROOM_HOME/refs/current.json" | sed "s#^#$MUSHROOM_HOME/#")" \
     > project/fixtures/current.jsonl
   ```

   This needs only whole-target `repo:read` authority. The asset normalizes Thing rows (including collections and assertions) into Mushroom's canonical format; record target, capture time, and artifact digest in `project/SPEC.md`. It is a review aid, not an authoritative snapshot once the target changes.

5. Run `prepare_fresh_sync` and inspect the persisted plan. On a populated target, unexpectedly large additions are a stop signal just as large revisions are: `adds + preserved` with no revisions/retractions can be silent identity duplication.
6. For a one-request plan, `validate_sync` creates a fresh live plan and asks WarmHub to evaluate it without writing. For larger plans, Mushroom has no read-only live-plan job; a separately approved `synchronize` run is the first live path and validates each batch just before applying it.
7. Rehearse the first live plan on a throwaway target or have a second reviewer inspect the identity decision and operations. After fresh readback proves stable identities, decide separately whether a complete source may authorize retractions.

Do not treat a zero-residual preserve plan as proof that unrelated stale records are gone. It proves only that the declared desired state and managed current state agree under the chosen policy.

## Upgrade a copied Mushroom project manually

This project pins `warmhub==0.27.4` and canonical `{name, shapes, data}` records. Upgrade the small number of template copies individually; there is no automatic updater or old-export translator.

1. Stop all writers and jobs sharing the target or `MUSHROOM_HOME`. Record the reviewed upstream commit and diff the copy's shared code against it. Preserve project transforms, asset prefixes, job configuration, acquisition behavior, and per-target artifact locations.
2. Preserve the existing artifact store and unresolved results. Resolve uncertain writes using fresh target reads; never replay old operation bytes. Regenerate old desired/current files and plans in native grammar.
3. Apply the coordinated reader, planner, submission, SDK/lockfile, fixture, and documentation changes together. Replace local workarounds only when the shared fix reproduces their regression case. Keep identities exactly as observed on the v2 server. If the old transform formatted Shape/id, change its rendering and typed references using the reviewed crosswalk to those observed names; do not infer identities by stripping arbitrary prefixes.
4. Update the transform to produce independent names, explicit declaration sets, and bodies. Move assertion subjects and collection fields into `data`. Update `MANAGED_SCOPE` to the actual names and record whole-Thing ownership. Supply reviewed effective `SHAPE_FIELDS` entries for every custom desired declaration, including `{}` for reference-free Shapes and inherited reference fields for composed Shapes. Confirm the approved target Shape versions and installed component schemas match those contracts before writes; historical validation certificates describe existing rows, not permission for new writes. The saved plan includes the reviewed contract map, which readback reconciliation reuses. Explicitly set `REQUIRE_EXISTING_RECORDS=True` for a populated migration, or `False` for a reviewed first load; omitting it fails planning. The setting records an operator decision, so an incorrectly chosen `False` cannot detect an empty capture caused by a wrong scope. Keep `ALLOW_SHAPE_CHANGES=False` unless a specific declaration transition has been reviewed.
5. Run frozen-vector tests and definition checks, then capture fresh live current state with approved read access. Review a no-change plan against populated state and one known change. A declaration migration must show its before/after declarations and retain expected versions.
6. Rehearse an approved apply on a throwaway target and require zero residual operations after fresh readback. Record digests, counts, run IDs, local deviations, and the upstream commit. Production rollout and resuming writers require the project's existing authorization and single-writer cutover.

For this project, see the NFL migration record in project/SPEC.md for observed names, reference contracts, offline checks, and remaining rollout gates.
