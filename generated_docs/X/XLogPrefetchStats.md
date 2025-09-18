# XLogPrefetchStats

## Location
src/backend/access/transam/xlogprefetcher.c: 171 - 185

## Overview
XLogPrefetchStats is a shared memory structure that maintains performance counters and metrics for the WAL prefetching system, exposed through the pg_stat_recovery_prefetch view.

## Definition


## Detailed Description
The XLogPrefetchStats structure serves as a comprehensive metrics collection system for PostgreSQL's WAL prefetching mechanism. It maintains both cumulative atomic counters for lifetime statistics and dynamic instant values for current system state. The structure is allocated in shared memory to allow multiple processes to update and read statistics concurrently. The atomic counters ensure thread-safe updates across different backend processes involved in WAL replay and recovery. These statistics are exposed to users through the pg_stat_recovery_prefetch system view for monitoring prefetch effectiveness.

## Parameters / Member Variables
- : pg_atomic_uint64 timestamp of when statistics were last reset/initialized
- : pg_atomic_uint64 counter of total prefetch operations initiated
- : pg_atomic_uint64 counter of blocks that were already present in cache when prefetch was attempted
- : pg_atomic_uint64 counter of zero-initialized (newly created) blocks that were skipped
- : pg_atomic_uint64 counter of new or missing blocks that were filtered out to avoid errors
- : pg_atomic_uint64 counter of Full Page Writes (FPW) that were skipped during prefetch
- : pg_atomic_uint64 counter of repeated block accesses that were skipped to avoid redundant I/O
- : int representing current number of WAL bytes the prefetcher is ahead of replay
- : int representing current number of block references ahead of replay position  
- : int representing current number of I/O operations in progress

## Dependencies
- Functions called/Symbols referenced:
  - pg_atomic_uint64 (atomic 64-bit unsigned integer type for thread-safe counters)

- Called from (representative examples):
  - XLogPrefetchShmemSize (calculates shared memory requirements)
  - XLogPrefetchShmemInit (initializes the shared memory structure)
  - XLogPrefetcherComputeStats (updates dynamic statistics)
  - Various prefetch functions that increment specific counters

## Notes and Other Information
The statistics structure is designed for high-concurrency access with atomic operations ensuring data consistency across multiple processes. The split between cumulative counters (using atomic types) and instant values (regular int) reflects their different usage patterns - counters are frequently updated from multiple contexts while instant values are typically updated by a single process. The reset_time field enables tracking of statistics collection periods. The various 'skip' counters help diagnose prefetch efficiency by categorizing why certain prefetch operations were avoided, providing insight into cache hit rates, filtering effectiveness, and system behavior during recovery operations.