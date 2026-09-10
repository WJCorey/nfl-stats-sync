"""agentgm/nfl-stats project automation (stopped by default).

Daily in-season synchronization at 06:30 America/New_York -- after nflverse's
nightly stat builds finish. The schedule ships STOPPED and must stay stopped
until the operator has: approved write credentials (WARMHUB_API_URL/WH_TOKEN),
durable DAGSTER_HOME and MUSHROOM_HOME, published canonical artifacts for every
ledger row, and a single-writer cutover for the target repository.

SINGLE-WRITER REQUIREMENT: exactly one live writer may run against
agentgm/nfl-stats at a time. Do not enable this schedule while the GitHub
Actions live-sync job (`.github/workflows/sync.yml`, gated by
LIVE_SYNC_APPROVED) or any manual `synchronize` launch can also write; pick one
writer and serialize the rest.
"""

import dagster as dg

from mushroom.defs.sync import synchronize

nflstats_daily_sync = dg.ScheduleDefinition(
    name="nflstats_daily_sync",
    job=synchronize,
    cron_schedule="30 6 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.STOPPED,
    run_config={"resources": {"warmhub_client": {"config": {"enabled": True}}}},
)
