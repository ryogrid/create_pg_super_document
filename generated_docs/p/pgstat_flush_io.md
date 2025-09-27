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
  - [PgStat_BktypeIO](../P/PgStat_BktypeIO.md)  
  - [LWLockConditionalAcquire](../L/LWLockConditionalAcquire.md)
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
  - [pgstat_report_vacuum](pgstat_report_vacuum.md)
  - [pgstat_report_analyze](pgstat_report_analyze.md)
  - [pgstat_report_wal](pgstat_report_wal.md)

## Notes and Other Information
- Returns false if no stats were recorded or if the flush was successful
- Returns true only when nowait is true and the lock could not be acquired
- Uses exclusive lightweight locks to protect shared statistics during updates
- Processes statistics in nested loops across I/O objects, contexts, and operations
- Accumulates both count and timing statistics from pending to shared memory
- Located in src/backend/utils/activity/pgstat_io.c:173-220

## Simplified Source

```c
// Simplified version of pgstat_flush_io
bool pgstat_flush_io(bool nowait) {
    // Early exit if no IO stats have been recorded
    if (!have_iostats)
        return false;

    // Get the lock and shared stats for this backend type
    LWLock *bktype_lock = &pgStatLocal.shmem->io.locks[MyBackendType];
    PgStat_BktypeIO *bktype_shstats = &pgStatLocal.shmem->io.stats.stats[MyBackendType];

    // Acquire lock (either blocking or non-blocking based on nowait)
    if (!nowait) {
        LWLockAcquire(bktype_lock, LW_EXCLUSIVE);
    } else if (!LWLockConditionalAcquire(bktype_lock, LW_EXCLUSIVE)) {
        return true;  // Could not acquire lock
    }

    // Transfer all pending stats to shared memory
    // Three nested loops for: IO objects, contexts, and operations
    for (int io_object = 0; io_object < IOOBJECT_NUM_TYPES; io_object++) {
        for (int io_context = 0; io_context < IOCONTEXT_NUM_TYPES; io_context++) {
            for (int io_op = 0; io_op < IOOP_NUM_TYPES; io_op++) {
                // Add pending counts to shared stats
                bktype_shstats->counts[io_object][io_context][io_op] +=
                    PendingIOStats.counts[io_object][io_context][io_op];

                // Add pending times to shared stats (converted to microseconds)
                instr_time time = PendingIOStats.pending_times[io_object][io_context][io_op];
                bktype_shstats->times[io_object][io_context][io_op] +=
                    INSTR_TIME_GET_MICROSEC(time);
            }
        }
    }

    // Release the lock
    LWLockRelease(bktype_lock);

    // Clear local pending stats and reset flag
    memset(&PendingIOStats, 0, sizeof(PendingIOStats));
    have_iostats = false;

    return false;  // Success
}
```

Key simplifications made:
- Removed assertion check for brevity (pgstat_bktype_io_stats_valid)
- Added descriptive comments explaining each major step
- Clarified the purpose of the triple nested loop
- Made the lock acquisition logic more readable
- Explained the return values in comments