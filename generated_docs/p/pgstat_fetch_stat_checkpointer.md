# pgstat_fetch_stat_checkpointer

## Location
[src/backend/utils/activity/pgstat_checkpointer.c:80-87](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_checkpointer.c#L80-L87)

## Overview
Retrieves a pointer to the current checkpointer statistics for SQL-callable functions to access checkpointer performance metrics.

## Definition
```c
PgStat_CheckpointerStats *pgstat_fetch_stat_checkpointer(void)
```

## Detailed Description
This function serves as a support function for SQL-callable pgstat* functions that need access to checkpointer statistics. It ensures the statistics snapshot is current by calling `pgstat_snapshot_fixed` with the checkpointer kind, then returns a pointer to the checkpointer statistics in the local snapshot. The function provides a standardized way to access checkpointer performance metrics including checkpoint counts, timing information, and buffer write statistics.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_snapshot_fixed](pgstat_snapshot_fixed.md)
  - PGSTAT_KIND_CHECKPOINTER
- Called from (representative examples):
  - [pg_stat_get_checkpointer_num_requested](pg_stat_get_checkpointer_num_requested.md)
  - [pg_stat_get_checkpointer_restartpoints_timed](pg_stat_get_checkpointer_restartpoints_timed.md)
  - [pg_stat_get_checkpointer_restartpoints_requested](pg_stat_get_checkpointer_restartpoints_requested.md)
  - [pg_stat_get_checkpointer_restartpoints_performed](pg_stat_get_checkpointer_restartpoints_performed.md)
  - [pg_stat_get_checkpointer_buffers_written](pg_stat_get_checkpointer_buffers_written.md)
  - [pg_stat_get_checkpointer_write_time](pg_stat_get_checkpointer_write_time.md)
  - [pg_stat_get_checkpointer_sync_time](pg_stat_get_checkpointer_sync_time.md)
  - [pg_stat_get_checkpointer_stat_reset_time](pg_stat_get_checkpointer_stat_reset_time.md)

## Notes and Other Information
- Located at src/backend/utils/activity/pgstat_checkpointer.c:80-87
- Returns a pointer to `pgStatLocal.snapshot.checkpointer` which contains statistics like checkpoint counts, timing data, and buffer write information
- The returned pointer provides access to a `PgStat_CheckpointerStats` structure containing metrics such as num_timed, num_requested, restartpoints_*, write_time, sync_time, buffers_written, and stat_reset_timestamp
- This function is thread-safe as it operates on the local snapshot after ensuring it's current

## Simplified Source

```c
PgStat_CheckpointerStats *
pgstat_fetch_stat_checkpointer(void)
{
    // Ensure statistics snapshot is current
    pgstat_snapshot_fixed(PGSTAT_KIND_CHECKPOINTER);

    // Return pointer to checkpointer stats in local snapshot
    return &pgStatLocal.snapshot.checkpointer;
}
```