# Mushroom

Mushroom is a library that simplifies publishing source data on a consistent basis to a WarmHub repository. Mushroom uses [Dagster](https://dagster.io/) to orchestrate data acquisition and processing, and uses WarmHub's Python SDK to submit updates.

To learn more about Dagster, visit the [Dagster documentation](https://docs.dagster.io/).

## Prerequisites

Install these tools before getting started:

- [Git](https://git-scm.com/downloads)
- [GitHub CLI](https://cli.github.com/) if you want to create the project from your terminal
- Python 3.10–3.14
- [`uv`](https://docs.astral.sh/uv/getting-started/installation/) for Python and dependency management
- [`jq`](https://jqlang.org/download/) for reviewing saved plans
- the [`wh` CLI](https://docs.warmhub.ai/get-started/) for creating and accessing WarmHub repositories

## Create a project from the template

Open the [Mushroom template](https://github.com/warmautomation/mushroom), select **Use this template**, and create a repository for your data project. You can also create and clone it with the GitHub CLI:

```bash
gh repo create <owner>/<repository> --private \
  --template warmautomation/mushroom --clone
```

After creating the repository, enter its directory and install the locked dependencies. If you used the browser, clone it first:

```bash
git clone https://github.com/<owner>/<repository>.git
cd <repository>
uv sync --frozen --group dev
```

The included project is a synthetic example. Replace it by following the [guidebook](guidebook/00-index.md) to connect Mushroom to a live WarmHub repository.

## Recommended authoring skills

We recommend installing these skills to enable your coding agent to make more effective design choices in WarmHub and Dagster. You can install them from the project directory:

```bash
npx skills add warmhub/warmhub-skills
npx skills add dagster-io/skills
```

- `warmhub/warmhub-skills` provides `design-warmhub-repo`, `plan-warmhub-ingestion`, and `wh-commit-design`.
- `dagster-io/skills` provides `dagster-expert`.

## Build your pipeline

Start with [`guidebook/00-index.md`](guidebook/00-index.md). It walks from source inspection through WarmHub repository design, implementation, review, synchronization, and recovery.

## View your pipeline in Dagster

After building the project, you may start the Dagster dashboard locally to view and coordinate your pipeline runs:

```bash
export DAGSTER_HOME="${DAGSTER_HOME:-$PWD/.dagster}"
mkdir -p "$DAGSTER_HOME"
uv run dg dev
```

By default, this appears at <http://127.0.0.1:3000>. Use the same `DAGSTER_HOME` in another terminal, then follow the [plan-review guide](guidebook/03-plan-review.md) to run the credential-free review. Use the reported run ID to find the run in Dagster and inspect its assets, logs, checks, and saved plan. The [operating guide](guidebook/05-operate.md) explains the other available jobs and when live WarmHub access is required.

## Automate and deploy

The guidebook in this repository allows you to build a functioning Dagster-to-WarmHub data pipeline locally. To explore options to deploy, automate, and schedule your data updates, review the recommended [automation and deployment paths](guidebook/05-operate.md#automate-after-local-success).
