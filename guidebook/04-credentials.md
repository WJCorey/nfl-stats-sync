# Enable live access

Normal tests, definition checks, and `prepare_sync` use no WarmHub credential. Only enable live access after the project and its prepared plan have been reviewed and the human approval checkpoint in `project/SPEC.md` is complete.

Before enabling a live credential, write down:

- environment and API URL;
- exact target `organization/repository`;
- managed scope;
- `repo:read` for complete current-state capture;
- `repo:write` for server validation or an approved synchronization; and
- how the credential will be supplied at runtime.

Record those exact details plus `Approved by` and `Approved on` in `project/SPEC.md`, then have a human approver approve live access explicitly. Repository/model approval alone does not authorize using a write-capable credential. This applies to both `validate_sync` and `synchronize`.

Use the current `wh` CLI workflow to authenticate and manage credentials for the approved operating context. Mushroom's current-state read needs repository-wide read access because it filters managed records locally. For an evaluation, use an isolated target rather than a credential restricted to a record-name pattern.

When upgrading an old credential, inspect its actual grants: v2 grants pair name paths with optional Shape restrictions, and old Shape-glob scope grammar may be refused on reissue. Replace affected grants using the current CLI grammar and the reviewed whole-Thing namespace; do not widen write authority merely to make capture work. Capture needs whole-target read access, including certified Shape schemas and referenced identities, without a restrictive Shape clause: Shape-restricted reads can hide inherited certificates. Keep write grants restricted to approved managed names. When writes use Shape clauses, one matching grant pair must authorize the complete declaration transition, including removed declarations where required; separate partial grants do not combine into that authority.

The SDK reads `WARMHUB_API_URL` and `WH_TOKEN`. Mushroom itself does not parse `.env`; `dg` loads a project-root `.env` for its commands, while another launcher must export the variables into its process. Keep values in an uncommitted runtime secret mechanism. Never put a token in a command argument, fixture, test, Dagster metadata, exception, log, commit, or pull request.

WarmHub's server-validation endpoint requires write authority even though it does not mutate the repository. Treat its credential as a write-capable secret:

- `validate_sync` needs read and write authority but does not submit operations;
- `synchronize` needs current-state read and write authority for the exact reviewed target and managed names.

Continue to [`05-operate.md`](05-operate.md) to validate, synchronize, inspect the Dagster run, and recover from failures.
