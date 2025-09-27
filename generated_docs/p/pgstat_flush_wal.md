# pgstat_flush_wal

## Location
[src/backend/utils/activity/pgstat_wal.c:82-109](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_wal.c#L82-L109)

## Overview
Calculates WAL usage counter differences and flushes the accumulated WAL statistics to shared memory, serving as the core mechanism for updating WAL statistics in PostgreSQL's statistics collection system.

## Definition
bool pgstat_flush_wal(bool nowait)

## Detailed Description
This function is responsible for calculating how much WAL usage counters have increased since the last flush by computing the difference between current and previous WAL usage counters. It then updates the shared memory statistics with these accumulated values.

The function performs several key operations:
1. Checks if there are pending WAL statistics to avoid unnecessary lock acquisition
2. Calculates WAL usage differences using WalUsageAccumDiff
3. Acquires the appropriate lock (conditional or blocking based on nowait parameter)
4. Updates shared memory statistics using accumulation macros
5. Saves current counters for the next calculation cycle
6. Clears the pending statistics buffer

The function uses optimized macros (WALSTAT_ACC and WALSTAT_ACC_INSTR_TIME) to efficiently update various WAL statistics fields including records, full page images, bytes, buffer operations, and timing information.

## Parameters / Member Variables
- : Boolean flag controlling lock acquisition behavior. When true, uses conditional lock acquisition and returns true if the lock cannot be acquired immediately. When false, blocks until the lock is acquired.

## Dependencies
- Functions called/Symbols referenced:
  - [PgStatShared_Wal](../P/PgStatShared_Wal.md)
  - [WalUsage](../W/WalUsage.md)
  - [pgstat_have_pending_wal](pgstat_have_pending_wal.md)
  - [WalUsageAccumDiff](../W/WalUsageAccumDiff.md)
  - [LWLockConditionalAcquire](../L/LWLockConditionalAcquire.md)
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [LWLockRelease](../L/LWLockRelease.md)
  - MemSet
- Called from (representative examples):
  - [pgstat_report_stat](pgstat_report_stat.md)
  - [pgstat_report_wal](pgstat_report_wal.md)

## Notes and Other Information
- Returns false on successful completion or when no pending statistics exist
- Returns true only when nowait is true and the lock could not be acquired
- Includes assertions to ensure proper postmaster environment and shared memory state
- Uses efficient macro-based accumulation for updating multiple statistics fields
- Maintains WAL usage counter history by saving current values as previous values
- Clears the PendingWalStats buffer after flushing to prepare for the next cycle
- Critical for maintaining accurate WAL activity measurements across all PostgreSQL processes
- The function is designed to be safe to call even when no WAL activity has occurred

## Simplified Source

```c
// Simplified version of pgstat_flush_wal
bool pgstat_flush_wal(bool nowait) {
    PgStatShared_Wal *shared_stats = &pgStatLocal.shmem->wal;
    WalUsage usage_diff = {0};

    // Early exit if no pending WAL statistics
    if (!pgstat_have_pending_wal()) {
        return false;
    }

    // Calculate difference between current and previous WAL usage
    WalUsageAccumDiff(&usage_diff, &pgWalUsage, &prevWalUsage);

    // Acquire lock (conditional or blocking based on nowait flag)
    if (nowait) {
        if (!LWLockConditionalAcquire(&shared_stats->lock, LW_EXCLUSIVE)) {
            return true;  // Could not acquire lock
        }
    } else {
        LWLockAcquire(&shared_stats->lock, LW_EXCLUSIVE);
    }

    // Update shared memory statistics with accumulated values
    shared_stats->stats.wal_records += usage_diff.wal_records;
    shared_stats->stats.wal_fpi += usage_diff.wal_fpi;
    shared_stats->stats.wal_bytes += usage_diff.wal_bytes;
    shared_stats->stats.wal_buffers_full += PendingWalStats.wal_buffers_full;
    shared_stats->stats.wal_write += PendingWalStats.wal_write;
    shared_stats->stats.wal_sync += PendingWalStats.wal_sync;
    shared_stats->stats.wal_write_time += INSTR_TIME_GET_MICROSEC(PendingWalStats.wal_write_time);
    shared_stats->stats.wal_sync_time += INSTR_TIME_GET_MICROSEC(PendingWalStats.wal_sync_time);

    LWLockRelease(&shared_stats->lock);

    // Save current counters for next calculation cycle
    prevWalUsage = pgWalUsage;

    // Clear statistics buffer for reuse
    MemSet(&PendingWalStats, 0, sizeof(PendingWalStats));

    return false;
}
```

Key simplifications made:
- Removed complex macro definitions for clearer inline operations
- Simplified variable names for better readability
- Removed detailed assertions (kept essential logic)
- Expanded macro calls to show actual operations
- Added descriptive comments for each major step
- Consolidated the lock acquisition logic into clearer if-else structure
- Focused on the main execution path