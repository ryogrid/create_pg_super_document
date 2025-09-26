# pgstat_slru_flush

## Location
[src/backend/utils/activity/pgstat_slru.c:156-173](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_slru.c#L156-L173)

## Overview
Flushes locally pending SLRU (Simple LRU) statistics entries to shared memory, accumulating the statistics from local pending counters into global shared statistics.

## Definition
```c
bool pgstat_slru_flush(bool nowait)
```

## Detailed Description
This function transfers SLRU statistics from local pending counters (`pending_SLRUStats`) to shared memory statistics (`pgStatLocal.shmem->slru.stats`). SLRU statistics track buffer cache operations for simple LRU buffer pools used by PostgreSQL subsystems like CLOG, MultiXact, and others.

The function operates by:
1. Checking if there are pending SLRU statistics to flush (`have_slrustats`)
2. Acquiring an exclusive lock on the shared SLRU statistics structure (either blocking or non-blocking based on `nowait`)
3. Iterating through all SLRU elements (`SLRU_NUM_ELEMENTS`) and accumulating pending statistics into shared counters
4. Clearing the local pending statistics and releasing the lock

The function uses a macro `SLRU_ACC(fld)` to accumulate each statistic field: `sharedent->fld += pendingent->fld`. This adds the local pending values to the corresponding shared memory counters for blocks_zeroed, blocks_hit, blocks_read, blocks_written, blocks_exists, flush, and truncate operations.

## Parameters / Member Variables
- `nowait`: Boolean flag controlling lock acquisition behavior
  - `false`: Function blocks until lock is acquired (always returns false on success)  
  - `true`: Function attempts non-blocking lock acquisition (returns true if lock could not be acquired, false on successful flush)

## Dependencies
- Functions called/Symbols referenced:
  - `LWLockAcquire`: Acquires exclusive lock on shared SLRU statistics (blocking)
  - `LWLockConditionalAcquire`: Attempts to acquire exclusive lock non-blocking
  - `LWLockRelease`: Releases the acquired lock
  - `MemSet`: Clears the pending statistics array
  - `PgStatShared_SLRU`: Shared memory structure containing SLRU statistics
  - `PgStat_SLRUStats`: Individual SLRU statistics structure
  - `SLRU_NUM_ELEMENTS`: Constant defining number of SLRU elements
  - `have_slrustats`: Global flag indicating if there are pending statistics
  - `pending_SLRUStats`: Local array of pending SLRU statistics
  - `pgStatLocal.shmem->slru`: Reference to shared SLRU statistics structure

- Called from (representative examples):
  - `pgstat_report_stat`: Main statistics reporting function that flushes various types of statistics

## Notes and Other Information
- The function handles memory synchronization between local process statistics and shared memory statistics used by the PostgreSQL statistics collector
- Return value semantics are counterintuitive: returns `false` on successful flush, `true` when lock could not be acquired (only in `nowait=true` mode)
- The function clears the `have_slrustats` flag after successfully flushing, indicating no pending local statistics remain
- SLRU statistics track buffer operations for PostgreSQL's various simple LRU caches (CLOG, MultiXact, etc.)
- Statistics accumulation is performed under exclusive lock protection to ensure consistency in multi-process environment
- The pending statistics array is statically allocated to avoid memory allocation within critical sections