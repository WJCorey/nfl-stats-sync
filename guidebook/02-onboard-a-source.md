# Onboard a source

Create `project/SPEC.md` while doing this work. It is the human-review record; `project/domain.py` and its tests remain executable truth.

## Design conversation

Work through the numbered design steps with the project owner so later choices build on earlier decisions. For each consequential choice, review the source or repository evidence, two or three viable options, their tradeoffs, and a recommendation. Record the answer in `project/SPEC.md`. Source facts that do not change the product can be recorded without turning them into questions.

When steps 1–6 are resolved, review and approve the project against the checkpoint below.

Source inspection and draft alternatives do not need approval. Do not implement the chosen transform, provision Shapes, request a write credential, or run against a live target until a human approver completes this checklist:

```markdown
## Human approval checkpoint

- [ ] Repository namespace, visibility, and purpose
- [ ] Product questions
- [ ] Shape names, kinds, fields, descriptions, and optionality
- [ ] Stable record-name formulas and the source of each identifier
- [ ] Relationships: what they connect, their direction, and what can be said about them
- [ ] `design-warmhub-repo` convergence checkpoint (or equivalent review) result and accepted limits
- [ ] Synchronization unit and stable slice key; which names this project may manage; whether the source is complete; and whether a missing record may be removed
- [ ] Source grain, ambiguities, and accepted information loss

Approved by:

Approved on:
```

Before approval, use the authoring guidance recommended by [`00-index.md`](00-index.md#before-you-begin) when it is available. Record the skills or canonical guidance consulted in `project/SPEC.md`.

## 1. State the questions

Write the questions the product must answer before choosing records. Do not add a record merely because a source happens to publish it.

## 2. Inspect real input

Read representative source rows or payloads before designing the transform. Record for every source:

| Question | Required answer |
| --- | --- |
| Access | URL, file, API, repository, authentication, and fallback |
| Grain | What one source row means |
| Coverage and synchronization unit | Complete current answer, independently meaningful slice, bounded slice (partial window), append-only, current-only, or unknown; correction mapping when sliced |
| Completion evidence | Terminal page/cursor, declared count, file boundary, or none |
| Provenance | Source URL, revision, timestamp, reporting period, or artifact digest |
| Failure behavior | Fail closed, or deliberately skip and record |
| Output | Which Things, relationships, and assertions this source contributes |

## 3. Draw the product

Use `design-warmhub-repo` when available with the product questions and inspected source evidence. If it is unavailable, record which repository-design guidance you used instead. Use that guidance to choose Things versus Assertions, identity, relationship structure, and each assertion's `about` target. Record the endpoint roles, traversal result, any cross-repo link behavior, and checkpoint result in `project/SPEC.md`. Review and approve that repo design before continuing; Mushroom should implement it without substituting a simpler primitive.

For every cross-source join, identify the durable key or crosswalk. If none exists, omit the edge and record the blocker rather than guessing.

Record the manually required target Shapes, including field names, types, optionality, and meaning. Do not provision Shapes from project code.

After the repo design is approved, use `plan-warmhub-ingestion` when available to review source access, mappings, idempotency, cadence, quality checks, and failure behavior. Record its decisions in `project/SPEC.md`; the next steps translate them into Mushroom's project interface and lifecycle limits.

## 4. Design identities

Implement the approved identity model in one naming authority in `project/domain.py`. For every record, write its Wref formula and normalization rules in `project/SPEC.md`.

For an existing target, decide whether that formula _continues_ its existing identities before writing a transform. Use the existing Wrefs only when their meaning and durable source keys demonstrably match. Otherwise write and review an explicit old-Wref-to-source-key crosswalk, or stop and start a separate scope. Do not mint cleaner source-derived names into a populated scope and mistake the resulting duplicate additions for a harmless backfill.

Choose the narrowest managed scope that covers the approved names. Use `*/**` only when this project explicitly owns every non-Shape record in the target repository.

If one narrow managed scope cannot cover all approved names without also including unrelated records, revise the names or split the product before approval.

Match identity grain to the real record: decide whether two observations at different times should revise one identity or coexist as history. If they must coexist, include a stable period, source segment ID, or other durable qualifier in the name. Search the inspected source for collisions under the proposed key; one happy-path row is not evidence of uniqueness.

Identity code must:

- reject missing, empty, or malformed identifiers;
- never turn `None` or another invalid value into a plausible name;
- produce the same name regardless of input order; and
- reject duplicate final Wrefs.

If no durable source identifier exists and identity uses correctable fields, document the compromise: what correction creates a new identity, what happens to the old record, and what future source capability would trigger migration.

## 5. Define semantic mappings

Document source-verbatim fields separately from normalized semantic fields. Unknown categories must fail or be preserved explicitly; never silently map an unknown role or status to a convenient default. Record any mapping policy whose meaning may change over time.

Do not reuse a source field name for a different concept.

## 6. Declare lifecycle behavior

First choose the synchronization unit using [the Mushroom model](01-model.md#choose-the-synchronization-unit). Then choose the managed scope, run-level completeness, and absence policy inside that unit. A complete page within a bounded slice does not prove that the whole managed corpus is complete. For grouped products, use the weakest coverage claim among required sources.

For an independently meaningful slice, record its stable source-derived key and correction mapping, then follow the proposed [semantic-partition pattern](07-semantic-partitions.md). Until that pattern ships, use the current flow only for preserve-only accumulation. If the product requires separate slice runs, backfills, or slice-local retractions, stop for a reviewed partition-aware implementation rather than adding a Dagster partition definition to `project/` alone.

Describe revisions at stable Wrefs. Describe what disappearance means. Use `preserve` unless absence is demonstrably meaningful and retraction is explicitly approved.

Mushroom applies one absence policy to every record in its managed scope. If different groups have different disappearance meanings—for example, permanent source evidence alongside rows that may be removed from a corrected complete snapshot—record that limitation in `project/SPEC.md`. Keep the combined product on `preserve`, or split it into separate projects with disjoint managed scopes. See [the current model limit](01-model.md#current-limit-one-rule-per-project).

## 7. Provision the approved repository

After a human completes the approval checkpoint, create the repository and provision the approved Shapes with `wh`. Verify the result with a read:

```bash
wh repo create <org>/<repo> --visibility private
wh shape create <ShapeName> --repo <org>/<repo> --fields '{"name":"string"}'
wh shape list --repo <org>/<repo>
```

Record the target and installed Shape definitions in `project/SPEC.md`. Mushroom does not create or synchronize Shapes.

## 8. Implement `project/`

`project/domain.py` supplies:

- `target()` — `MUSHROOM_TARGET` when set, otherwise the harmless synthetic example coordinate. Set `MUSHROOM_TARGET` before any live job in a copied project; never use the example fallback as a live target;
- `MANAGED_SCOPE` — the single managed Wref glob;
- `ABSENCE_POLICY` — `preserve` or `retract`;
- `source_fixture()` and `current_state_fixture()` — local frozen vectors; and
- `transform(source: bytes)` — deterministic operation-neutral records.

`project/acquisition.py` supplies `acquire() -> bytes`. Write ordinary project-owned Python and use whichever library the source requires. Return the exact frozen bytes that `transform()` will consume. The module must remain importable for fixture-only runs; `source_snapshot` calls `acquire()` for `prepare_fresh_sync`, `validate_sync`, and `synchronize`, and reads the frozen fixture otherwise. It must fail closed on unavailable, empty, malformed, or incoherently resolved input; there is no per-file moving fallback.

Replace `tests/test_domain.py` and `tests/test_acquisition.py` with tests for the product you put in `project/`; they are synthetic examples. Keep the shared kernel/lifecycle tests (`test_kernel`, `test_current_state`, `test_submission`, and `test_sync`) unchanged.

The frozen source is a JSON object with a top-level boolean `complete` field. That value is the coverage claim for this exact snapshot.

Do not import Dagster into the transform. Do not emit WarmHub operations from project code. Let exceptions stop planning when input cannot support a safe claim.

Mushroom scopes use literal path segments plus whole-segment `*` and `**`. `*` matches one segment; trailing `/**` requires a descendant. Do not use brace, extglob, character-class, or partial-segment globs.

## 9. Leave evidence

Freeze source, current-state, desired-state, and expected-operation vectors. Tests should prove at least:

- the exact desired identities and relationship endpoints;
- two records at the identity boundary, including repeated entities across periods or multiple segments inside one period when the source permits them;
- deterministic output when source order changes;
- rejection of malformed identifiers, duplicate identities, and unknown semantic values;
- the full plan summary: adds, revisions, retractions, unchanged, and preserved;
- partial or failed acquisition cannot cause retractions; and
- any documented unstable-identity behavior.

The finished `project/SPEC.md` should contain: the completed human approval checkpoint, boundary, source inventory, product dependency order, manual Shape prerequisites, identity formulas, mapping rules, completeness and absence policy, frozen vectors, known limitations, evaluation evidence, the questions it answers, time to first valid plan, and the guidebook and skill guidance consulted.

Next, run the credential-free review and inspect its saved operations using [`03-plan-review.md`](03-plan-review.md).
