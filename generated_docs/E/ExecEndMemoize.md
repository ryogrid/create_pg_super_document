# ExecEndMemoize

## Location
src/backend/executor/nodeMemoize.c: 1080 - 1139

## Overview
ExecEndMemoize performs cleanup and shutdown operations for a MemoizeState node, including memory validation, parallel worker statistics collection, and resource deallocation.

## Definition


## Detailed Description
ExecEndMemoize handles the proper shutdown of a Memoize execution node. In debug builds, it performs comprehensive memory accounting validation by iterating through all cache entries and their associated tuples to verify that the tracked memory usage matches the actual memory consumption. For parallel query execution, it copies the worker's accumulated statistics (cache hits, misses, overflows, and peak memory usage) back to shared memory so the main process can aggregate and report them in EXPLAIN ANALYZE output. Finally, it performs cleanup by deleting the dedicated memory context that holds all cache data and recursively shutting down the outer plan node.

## Parameters / Member Variables
- `node`: The MemoizeState node to shut down, containing the hash table, statistics, memory context, and outer plan reference

## Dependencies
- Functions called/Symbols referenced:
  - memoize_start_iterate
  - memoize_iterate
  - EMPTY_ENTRY_MEMORY_BYTES
  - CACHE_TUPLE_BYTES
  - IsParallelWorker
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
  - [ExecEndNode](ExecEndNode.md)
  - outerPlanState
- Called from (representative examples):
  - [ExecEndNode](ExecEndNode.md) (main node cleanup dispatcher)

## Notes and Other Information
- Memory validation in assert builds helps catch memory accounting bugs during development by comparing tracked usage against actual consumption
- Handles parallel execution by copying worker statistics to shared memory for aggregation in the main process
- Sets mem_peak statistic if it wasn't already recorded during execution, ensuring EXPLAIN ANALYZE shows accurate peak memory usage
- Deletes the entire "MemoizeHashTable" memory context, which automatically frees all cached entries and tuples
- Ensures proper cleanup chain by calling ExecEndNode on the outer plan node
- Uses conditional compilation (#ifdef USE_ASSERT_CHECKING) to include expensive validation only in debug builds