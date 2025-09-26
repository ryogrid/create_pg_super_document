# pgstat_archiver_snapshot_cb

## Location
[src/backend/utils/activity/pgstat_archiver.c:81-111](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_archiver.c#L81-L111)

## Overview
Callback function that creates a local snapshot of archiver statistics, applying reset offsets to provide delta values since the last statistics reset.

## Definition
```c
void pgstat_archiver_snapshot_cb(void)
```

## Detailed Description
This callback function creates a consistent snapshot of archiver statistics for local consumption. It first copies the current statistics from shared memory using the changecount mechanism to ensure consistency, then acquires a shared lock to read the reset offset values. The function then applies the reset offset logic by subtracting the baseline values established during the last reset from the current statistics. For cases where the current count equals the reset count (indicating no activity since reset), it clears the associated metadata fields (WAL filename and timestamp) to indicate no recent activity.

## Parameters / Member Variables
None - this function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [PgStatShared_Archiver](../P/PgStatShared_Archiver.md)
  - [PgStat_ArchiverStats](../P/PgStat_ArchiverStats.md)
  - [pgstat_copy_changecounted_stats](pgstat_copy_changecounted_stats.md)
  - LW_SHARED
- Called from (representative examples):
  - SH_DECLARE (via statistics framework callback mechanism)

## Notes and Other Information
The function implements PostgreSQL's reset-offset statistics model where statistics continue accumulating in shared memory but local snapshots show incremental values since the last reset. The special handling for cases where counts match reset values (clearing WAL filename and timestamp) ensures that SQL functions don't report stale metadata from before the last reset when no activity has occurred since then.