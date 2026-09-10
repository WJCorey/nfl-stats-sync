# Operate a project

Mushroom exposes four named Dagster jobs. For an existing target, follow [`06-adopt-an-existing-repo.md`](06-adopt-an-existing-repo.md) before treating any fixture plan as evidence.

## Prepare without credentials

`prepare_sync` is the normal authoring loop. Follow [`03-plan-review.md`](03-plan-review.md) for the exact first-run checks, command, and saved-plan review.

## Prepare with fresh source, still without credentials

```bash
uv run dg launch --job prepare_fresh_sync
```

This runs `project/acquisition.py`'s `acquire()` against the live public source, freezes that exact input, and plans it against the fixture current state. No WarmHub credential is read. Use it to review what a real acquisition would propose before enabling any live job. It is not a preview of the current production target.

## Validate without writing

Create a temporary, uncommitted Dagster config:

```yaml
# live.yaml — uncommitted
resources:
  warmhub_client:
    config:
      enabled: true
```

This enables the live jobs below. The separate `live-read.yaml` in [`06-adopt-an-existing-repo.md`](06-adopt-an-existing-repo.md) enables only the `current_state` asset for a local rehearsal.

With `WARMHUB_API_URL`, `WH_TOKEN`, and `MUSHROOM_TARGET` supplied at runtime:

```bash
uv run dg launch --job validate_sync --config live.yaml
```

This captures live current state, makes a fresh checked plan, and asks WarmHub to evaluate those exact operations without writing. It is advisory: a later write does not reuse this result as authorization.

## Synchronize with one button

After the repository/model checkpoint and explicit write approval, use the same resource config:

```bash
uv run dg launch --job synchronize --config live.yaml
```

The job makes a fresh plan immediately before writing, so it never reuses an old plan after the repository has changed. It validates and submits that plan in ordered batches, reads live state again, and requires no remaining updates. A zero-operation plan skips the write and still checks the result.

Standalone validation uses one server request and therefore stops clearly above WarmHub's request limits. Synchronization instead validates each ordered batch immediately before applying that same batch; it does not infer a projected server state from independent dry runs.

The reviewed `prepare_*` fixture plan is not the submitted plan. `synchronize` always reacquires source, rereads current state, replans, then submits fresh checked bytes so it cannot replay stale expected versions.

## Inspect a run in Dagster

After every material `dg launch`, record the job name, run ID, and result, and make the run available in a Dagster UI.

An existing UI is compatible only when it uses the same `DAGSTER_HOME` and its workspace loads Mushroom. A URL responding on port 3000 is not enough evidence; do not scan ports or silently attach to another project. Confirm its instance and workspace before reusing it.

Otherwise, use the same persistent local instance in every terminal and keep Mushroom's UI open in a second terminal:

```bash
export DAGSTER_HOME="${DAGSTER_HOME:-$PWD/.dagster}"
mkdir -p "$DAGSTER_HOME"
uv run dg dev
```

Use or share <http://127.0.0.1:3000> and the run ID for inspection. If that port is in use, choose between reusing the compatible instance and starting Mushroom on an alternative port with `uv run dg dev --port <port>`. Keep the UI URL with the run record.

## Failure recovery

If validation fails, fix the source, mapping, target prerequisites, or current state and start a new run.

If submission is partial, interrupted, or uncertain:

1. stop;
2. keep any persisted per-operation results;
3. capture fresh current state;
4. create and review a new plan; and
5. never replay the old operation bytes.

Results are persisted only when the running process observes a server result or caught failure. A hard process loss may leave no result artifact; the same fresh-read-and-replan recovery applies.

An obvious failed run is recoverable. Mushroom does not promise to reconstruct an unavailable server receipt.

## Evaluation receipt

For an approved throwaway target, record only safe evidence: target identity, source/desired/current/plan/results/readback/residual digests, plan and validation counts, Dagster run and submission identifiers, desired and readback counts, and residual operation count. Do not record credential values.

On a clean target, require desired count to equal fresh readback count, zero residual operations, and zero preserved records. Under `preserve`, an empty residual plan alone can hide unrelated stale managed records.

## Automate after local success

Deployment is optional. Choose it only after the project works manually and its desired cadence is understood; source acquisition and transformation code stay the same across these paths.

Mushroom does not currently ship a schedule or deployment files. The paths below are operating choices, not one-click deployments. Project-specific schedules, sensors, and checks may use the definition-discovery seam described below after a successful manual synchronization; leave them stopped until their credentials, cadence, and per-target writer serialization have been reviewed.

| Path | Best fit | Observability | What you operate |
| --- | --- | --- | --- |
| Local Dagster | Manual or occasional runs | Local Dagster UI and run history | The local process and persistent `DAGSTER_HOME` and `MUSHROOM_HOME` |
| [GitHub Actions](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule) | Simple manual, daily, or weekly hosted runs | GitHub run status, logs, notifications, and uploaded Mushroom artifacts | A target-scoped secret and workflow; scheduled starts are best-effort |
| [Dagster OSS on one persistent machine](https://docs.dagster.io/deployment/oss/deployment-options/deploying-dagster-as-a-service) | An always-on machine needing Dagster schedules and UI | Persistent Dagster UI, runs, and event logs | Webserver, daemon, code location, instance storage, and persistent `MUSHROOM_HOME`; use a service manager or [Docker Compose](https://docs.dagster.io/deployment/oss/deployment-options/docker) |
| [Dagster+ Hybrid](https://docs.dagster.io/deployment/dagster-plus/hybrid) | Managed UI, schedules, and alerts with execution on a persistent user-operated host | Managed control plane and run history | Hybrid agent, deployment configuration, credentials, [a plan with enough user seats](https://dagster.io/pricing), and persistent Mushroom artifacts on the execution host |

For GitHub Actions, use both a manual button and the product's reviewed cron, serialize runs for the target repository, do not cancel an active synchronization, and retain `.mushroom/` as an access-controlled workflow artifact with a reviewed retention period on success or failure. GitHub schedules may start late or be dropped under load, so use them only when the next run can safely catch up.

### Native Dagster schedule: happy path

Use a Dagster schedule when the project has an always-on Dagster deployment and needs the same code location to own its schedule, sensors, or asset checks. The definition loader discovers modules under `src/mushroom/defs/`, so put project-specific automation in a clearly named sibling of `sync.py`; do not modify `sync.py` or `definitions.py` for it.

For example, this runs the existing `synchronize` job at 9:00 AM Monday and Thursday in New York, but remains stopped after deployment:

```python
# src/mushroom/defs/fishkill_automation.py
"""Fishkill project automation; do not enable before the single-writer cutover."""

import dagster as dg

from mushroom.defs.sync import synchronize


fishkill_sync = dg.ScheduleDefinition(
    name="fishkill_sync",
    job=synchronize,
    cron_schedule=["0 9 * * 1", "0 9 * * 4"],
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.STOPPED,
    run_config={"resources": {"warmhub_client": {"config": {"enabled": True}}}},
)
```

`ScheduleDefinition` defaults to stopped; setting it explicitly makes the safety boundary visible in review. A Dagster daemon evaluates schedules and sensors, so the module alone does not run anything. Enable it only after the project has approved write credentials, durable `DAGSTER_HOME` and `MUSHROOM_HOME`, a single-writer cutover, and a deployed daemon. Keep project-specific freshness checks and stopped-by-default sensors in this same module when they share that lifecycle.

For a persistent Dagster deployment, run one daemon and one or more webservers against the same Dagster instance and workspace. Do not expose the Dagster OSS webserver directly to the internet; keep it on a private network such as Tailscale or another VPN, or put an authentication layer such as [Cloudflare Tunnel and Access](https://developers.cloudflare.com/cloudflare-one/access-controls/applications/http-apps/self-hosted-public-app/) in front of it. The webserver's `--read-only` option disables launches and schedule changes, but it is not authentication.

Keep code and state separate across upgrades. An image or virtual environment contains code and dependencies. `DAGSTER_HOME` (or a configured database) stores Dagster run metadata, while `MUSHROOM_HOME` stores checked snapshots, plans, and results. Put both state locations on persistent host paths or volumes, never only in an image or container's writable layer, and back them up together. Dagster+ Serverless remains unsuitable until Mushroom has durable artifact storage outside an ephemeral worker.
