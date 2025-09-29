# XLogPrefetcherComputeStats

## Location
[src/backend/access/transam/xlogprefetcher.c:412-460](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogprefetcher.c#L412-L460)

## Overview
Computes and updates real-time statistics for WAL prefetch operations that are visible through the pg_stat_recovery_prefetch system view.

## Definition
```c
void XLogPrefetcherComputeStats(XLogPrefetcher *prefetcher)
```

## Detailed Description
XLogPrefetcherComputeStats calculates current WAL prefetch performance metrics and updates the shared memory statistics visible to users through the pg_stat_recovery_prefetch view. The function computes three key metrics: WAL distance (how far ahead the prefetcher is reading compared to replay), I/O depth (number of outstanding read requests), and block distance (total blocks being processed).

The function determines WAL distance by examining the decode queue's head and tail LSN positions. I/O metrics are obtained from the LSN read queue to show both inflight and completed operations. These statistics provide insight into prefetch efficiency and help diagnose performance issues during WAL recovery.

After updating the statistics, the function schedules the next statistics update by setting next_stats_shm_lsn based on the current read position plus a predefined distance interval.

## Parameters / Member Variables
- `prefetcher`: Pointer to the XLogPrefetcher instance for which to compute statistics

## Dependencies
- Functions called/Symbols referenced:
  - [lrq_inflight](../l/lrq_inflight.md) (gets number of pending I/O operations)
  - [lrq_completed](../l/lrq_completed.md) (gets number of completed I/O operations)
  - XLOGPREFETCHER_STATS_DISTANCE (constant defining update interval)
- Called from (representative examples):
  - [XLogPrefetcherReadRecord](XLogPrefetcherReadRecord.md) (during regular record processing)
  - [ShutdownWalRecovery](../S/ShutdownWalRecovery.md) (during recovery shutdown)
  - [WaitForWALToBecomeAvailable](../W/WaitForWALToBecomeAvailable.md) (during WAL availability checks)

## Notes and Other Information
- Updates three key shared memory statistics: wal_distance, io_depth, and block_distance
- WAL distance calculation safely handles cases where decode_queue_tail is NULL (returns 0)
- Block distance represents the sum of inflight and completed I/O operations, showing total prefetch activity
- Statistics updates are throttled using XLOGPREFETCHER_STATS_DISTANCE to avoid excessive overhead
- The computed statistics are immediately visible through PostgreSQL's statistics views, providing real-time monitoring capabilities
- Essential for monitoring WAL recovery performance and diagnosing prefetch effectiveness

## Simplified Source

```c
// Simplified version of XLogPrefetcherComputeStats
void XLogPrefetcherComputeStats(XLogPrefetcher *prefetcher) {
    uint32 io_depth;
    uint32 completed;
    int64 wal_distance;

    // Calculate WAL distance: how far ahead of replay we are
    if (prefetcher->reader->decode_queue_tail) {
        wal_distance = prefetcher->reader->decode_queue_tail->lsn -
                      prefetcher->reader->decode_queue_head->lsn;
    } else {
        wal_distance = 0;
    }

    // Get current I/O statistics from streaming read queue
    io_depth = lrq_inflight(prefetcher->streaming_read);    // Pending I/Os
    completed = lrq_completed(prefetcher->streaming_read);  // Completed I/Os

    // Update shared memory statistics visible in pg_stat_recovery_prefetch
    SharedStats->io_depth = io_depth;
    SharedStats->block_distance = io_depth + completed;  // Total prefetch activity
    SharedStats->wal_distance = wal_distance;

    // Schedule next statistics update
    prefetcher->next_stats_shm_lsn =
        prefetcher->reader->ReadRecPtr + XLOGPREFETCHER_STATS_DISTANCE;
}
```

Key simplifications made:
- Added descriptive comments explaining each major step
- Clarified the purpose of each statistic being computed
- Made variable usage more explicit with inline comments
- Simplified the overall structure while preserving all essential logic
- Maintained the exact algorithm and data flow of the original function