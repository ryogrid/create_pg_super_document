# pgstat_flush_io

## Location
[src/backend/utils/activity/pgstat_io.c:173-220](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_io.c#L173-L220)

## Overview
Flushes locally pending I/O statistics from the current backend to the shared statistics memory.

## Definition

```c
bool
pgstat_flush_io(bool nowait)
```
## Detailed Description
This function transfers accumulated I/O statistics from the local backend's pending statistics to the shared statistics memory where they can be accessed by other processes. The function operates on three-dimensional arrays of statistics data organized by I/O object, I/O context, and I/O operation types. It uses lightweight locks to ensure thread-safe access to the shared statistics. The function supports both blocking and non-blocking modes based on the nowait parameter. After successfully flushing the statistics, it clears the local pending statistics and resets the have_iostats flag.

## Parameters / Member Variables
- `nowait`: If true, the function returns immediately if it cannot acquire the required lock; if false, it waits for the lock

## Dependencies
- Functions called/Symbols referenced:
  - [LWLock](../L/LWLock.md)
  - PgStat_BktypeIO  
  - LWLockConditionalAcquire
  - IOOBJECT_NUM_TYPES
  - IOCONTEXT_NUM_TYPES
  - IOOP_NUM_TYPES
  - [instr_time](../i/instr_time.md)
  - INSTR_TIME_GET_MICROSEC
  - [pgstat_bktype_io_stats_valid](pgstat_bktype_io_stats_valid.md)
- Called from (representative examples):
  - [WalSndWaitForWal](../W/WalSndWaitForWal.md)
  - [WalSndLoop](../W/WalSndLoop.md)
  - [pgstat_report_stat](pgstat_report_stat.md)
  - pgstat_report_vacuum
  - pgstat_report_analyze
  - pgstat_report_wal

## Notes and Other Information
- Returns false if no stats were recorded or if the flush was successful
- Returns true only when nowait is true and the lock could not be acquired
- Uses exclusive lightweight locks to protect shared statistics during updates
- Processes statistics in nested loops across I/O objects, contexts, and operations
- Accumulates both count and timing statistics from pending to shared memory
- Located in src/backend/utils/activity/pgstat_io.c:173-220