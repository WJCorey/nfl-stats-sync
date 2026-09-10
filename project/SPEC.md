# Synthetic starter

This project exists only to keep a fresh Mushroom clone runnable without implying a real repository, source, or deployment. It is not approved for live use. Replace it when authoring a knowledge product.

## Human approval checkpoint

Do not mark these complete for the synthetic example. A real project must record explicit human approval before implementing its transform, provisioning Shapes, requesting write credentials, or starting a live job.

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

Only the human approver may complete this checkpoint.

## Example contract

- Target fallback: `example/mushroom-starter` (metadata only; never use live)
- Managed scope: `ExampleRecord/example/**`
- Coverage: `complete=false`
- Absence policy: `preserve`
- Required manual Shape: `ExampleRecord` with required string field `label`
- Identity: `ExampleRecord/example/{validated-source-id}`
- Question demonstrated: which labeled example records are in this snapshot?

The fixture contains two desired records. Fixture current state makes one an addition, one a revision, and leaves one current-only record preserved. This demonstrates the planning interface without being an answer key for a real knowledge product.

## Guidance for replacement

Use the authoring guidance recommended by [`guidebook/00-index.md`](../guidebook/00-index.md#before-you-begin). Record the guidance consulted, source evidence, human decisions, frozen vectors, and evaluation receipt in the replacement specification.
