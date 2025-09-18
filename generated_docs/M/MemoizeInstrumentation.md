# MemoizeInstrumentation

## Location
src/include/nodes/execnodes.h: 2238 - 2251

## Overview
MemoizeInstrumentation is a structure that tracks performance metrics and statistics for memoization operations in PostgreSQL query execution.

## Definition
```c
typedef struct MemoizeInstrumentation
{
    uint64      cache_hits;         /* number of rescans where weve found the
                                     * scan parameter values to be cached */
    uint64      cache_misses;       /* number of rescans where weve not found the
                                     * scan parameter values to be cached. */
    uint64      cache_evictions;    /* number of cache entries removed due to
                                     * the need to free memory */
    uint64      cache_overflows;    /* number of times weve had to bypass the
                                     * cache when filling it due to not being
                                     * able to free enough space to store the
                                     * current scans tuples. */
    uint64      mem_peak;           /* peak memory usage in bytes */
} MemoizeInstrumentation;
```

## Detailed Description
MemoizeInstrumentation collects detailed performance metrics for memoization nodes during query execution. Memoization is an optimization technique where PostgreSQL caches the results of subplans based on their input parameters, allowing subsequent executions with the same parameters to return cached results instead of re-executing the subplan. This structure tracks various aspects of cache performance to help analyze the effectiveness of memoization.

The instrumentation data is particularly valuable for query optimization analysis, EXPLAIN output, and performance monitoring. It helps database administrators and query planners understand how well memoization is working for specific queries and whether the cache size and eviction policies are appropriate for the workload.

## Parameters / Member Variables
- `cache_hits`: Number of rescans where the scan parameter values were found in the cache, allowing immediate result retrieval
- `cache_misses`: Number of rescans where the scan parameter values were not found in the cache, requiring subplan execution
- `cache_evictions`: Number of cache entries that were removed to free memory for new entries due to cache size limits
- `cache_overflows`: Number of times the cache was bypassed because there wasnt enough space to store the current scans tuples
- `mem_peak`: Maximum memory usage in bytes reached by the memoization cache during execution

## Dependencies
- Functions called/Symbols referenced:
  - (No direct dependencies - this is a pure data structure)
- Called from (representative examples):
  - show_memoize_info (for EXPLAIN output)
  - ExecMemoizeRetrieveInstrumentation
  - SharedMemoizeInfo (for parallel query execution)
  - MemoizeState (contains instrumentation data)

## Notes and Other Information
- Used by EXPLAIN (ANALYZE, BUFFERS) to show memoization performance statistics
- Cache hit ratio can be calculated as cache_hits / (cache_hits + cache_misses) to measure memoization effectiveness
- High cache_evictions may indicate that work_mem or hash_mem_multiplier settings are too low for the workload
- Cache overflows suggest that individual scan results are too large to fit in the available cache memory
- The structure supports both local and parallel query execution through SharedMemoizeInfo
- All counters use uint64 to handle high-volume query workloads without overflow
- Peak memory tracking helps identify memory pressure and optimize cache sizing