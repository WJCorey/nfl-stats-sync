# Review a plan

Run the credential-free path first:

```bash
uv sync --frozen --group dev
uv run pytest
uv run dg check defs
uv run dg check toml
export DAGSTER_HOME="${DAGSTER_HOME:-$PWD/.dagster}"
mkdir -p "$DAGSTER_HOME"
export MUSHROOM_TARGET=organization/repository
uv run dg launch --job prepare_sync
```

Replace `organization/repository` with the intended target. Both parts may contain lowercase letters, digits, periods, and hyphens. This credential-free run does not contact WarmHub; the target is recorded in the plan so reviewers can verify its destination. Use the same `DAGSTER_HOME` in any terminal that starts the Dagster UI so the run appears there.

`prepare_sync` freezes the configured fixture source, produces desired state, compares it with fixture current state, persists one canonical `operations.jsonl`, and checks that the plan manifest names those exact bytes.

Review both the summary and the operations themselves. The summary must account for adds, revisions, retractions, unchanged records, and preserved records. Inspect every retraction and any unexpectedly large addition, revision, or preserve count. In a populated target, a large add count is a red flag: it can mean the transform minted new identities instead of revising the target's existing records. `adds + preserved` with no revisions or retractions is not a clean result until the existing-identity/crosswalk decision is reviewed.

Mushroom saves the exact plan beneath `MUSHROOM_HOME` (default `.mushroom`). Review that saved plan, not a plan recreated later or copied from console output.

```bash
export MUSHROOM_HOME="${MUSHROOM_HOME:-.mushroom}"
jq . "$(jq -r '.key' "$MUSHROOM_HOME/refs/plan.json" | sed "s#^#$MUSHROOM_HOME/#")"
jq -s 'group_by(.operation) | map({operation: .[0].operation, count: length})' \
  "$(jq -r '.key' "$MUSHROOM_HOME/refs/operations.json" | sed "s#^#$MUSHROOM_HOME/#")"
```

The first command opens the plan details—including its target, managed scope, absence policy, and summary counts. The second counts each kind of planned operation.

Collection adds deliberately use a bare collection name on the WarmHub wire; collection revisions and retractions use their full Wref. Review the persisted operation bytes, not an assumed reconstruction of that projection.

Ask these questions before live validation:

1. Are all operation names inside the declared managed scope?
2. Do names come only from approved stable identity inputs?
3. Do revisions preserve identity and carry expected versions?
4. Are relationship endpoints correct and dependency order sensible?
5. Does every retraction have complete coverage and explicit authorization?
6. Are preserved records expected consequences of incomplete coverage?
7. Does the target in the plan manifest match the intended repository?

A zero-retraction plan can still be semantically wrong. A human reviews what the records mean, not only whether the files are valid.

`prepare_sync` and `prepare_fresh_sync` use `project/fixtures/current.jsonl`. They are useful code and source rehearsals, but not a preview of a populated production target unless that fixture was deliberately refreshed from it. A later `synchronize` run acquires fresh source, reads fresh current state, replans, and applies those new bytes; it never applies this reviewed fixture plan. That protects expected versions from stale replay, but means review approves the mapping and safety policy rather than authorizing immutable bytes.

After the project and plan are approved, continue to [`04-credentials.md`](04-credentials.md) to enable narrowly scoped live access.
