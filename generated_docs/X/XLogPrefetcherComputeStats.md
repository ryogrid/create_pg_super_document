# XLogPrefetcherComputeStats

## Location
src/backend/access/transam/xlogprefetcher.c: 412 - 460

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
  - lrq_inflight (gets number of pending I/O operations)
  - lrq_completed (gets number of completed I/O operations)
  - XLOGPREFETCHER_STATS_DISTANCE (constant defining update interval)
- Called from (representative examples):
  - XLogPrefetcherReadRecord (during regular record processing)
  - ShutdownWalRecovery (during recovery shutdown)
  - WaitForWALToBecomeAvailable (during WAL availability checks)

## Notes and Other Information
- Updates three key shared memory statistics: wal_distance, io_depth, and block_distance
- WAL distance calculation safely handles cases where decode_queue_tail is NULL (returns 0)
- Block distance represents the sum of inflight and completed I/O operations, showing total prefetch activity
- Statistics updates are throttled using XLOGPREFETCHER_STATS_DISTANCE to avoid excessive overhead
- The computed statistics are immediately visible through PostgreSQL's statistics views, providing real-time monitoring capabilities
- Essential for monitoring WAL recovery performance and diagnosing prefetch effectiveness