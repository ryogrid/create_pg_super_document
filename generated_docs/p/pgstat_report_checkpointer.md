# pgstat_report_checkpointer

## Location
[src/backend/utils/activity/pgstat_checkpointer.c:30-48](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_checkpointer.c#L30-L48)

## Overview
Reports checkpointer and IO statistics by transferring accumulated statistics from local buffers to shared memory for system-wide visibility.

## Definition
```c
void pgstat_report_checkpointer(void)
```

## Detailed Description
This function serves as the primary mechanism for reporting checkpointer statistics to PostgreSQL's statistics collection system. It transfers accumulated statistics from the local `PendingCheckpointerStats` buffer to shared memory, making them available for monitoring and analysis. The function implements an optimization to avoid unnecessary shared memory updates when no statistics have been accumulated, and includes proper concurrency control using change counters to ensure atomic updates.

The function accumulates various checkpointer metrics including:
- Number of timed and requested checkpoints
- Restart point statistics for standby servers
- Buffer write and sync timing information
- Number of buffers written during checkpoints

After transferring statistics, it clears the local buffer and reports IO statistics through the pgstat_report_io() mechanism.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_assert_is_up](pgstat_assert_is_up.md)
  - [pgstat_begin_changecount_write](pgstat_begin_changecount_write.md)
  - [pgstat_end_changecount_write](pgstat_end_changecount_write.md)
  - memcmp
  - MemSet
  - pgstat_report_io
- Types referenced:
  - [PgStat_CheckpointerStats](../P/PgStat_CheckpointerStats.md)
  - [PgStatShared_Checkpointer](../P/PgStatShared_Checkpointer.md)
- Called from (representative examples):
  - [CheckpointerMain](../C/CheckpointerMain.md)
  - [HandleCheckpointerInterrupts](../H/HandleCheckpointerInterrupts.md)
  - [CheckpointWriteDelay](../C/CheckpointWriteDelay.md)

## Notes and Other Information
- Uses a static zero-initialized structure for efficient comparison to detect if any statistics have been accumulated
- Implements atomic updates using change counters to ensure consistency in multi-process environments
- Always clears the local statistics buffer after reporting to prevent double-counting
- Part of PostgreSQL's comprehensive statistics collection framework for monitoring system performance
- The function is designed to be called frequently from checkpointer processes without significant performance overhead