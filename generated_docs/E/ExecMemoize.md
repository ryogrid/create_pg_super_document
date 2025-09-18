# ExecMemoize

## Location
src/backend/executor/nodeMemoize.c: 697 - 951

## Overview
ExecMemoize is the main execution function for the Memoize node that caches and reuses query results based on parameter values to avoid redundant computation of expensive subplans.

## Definition


## Detailed Description
ExecMemoize implements a sophisticated caching mechanism for PostgreSQL's execution engine. It operates through a state machine with five distinct states:

1. **MEMO_CACHE_LOOKUP**: Initial state where it checks if results for current parameters are already cached
2. **MEMO_CACHE_FETCH_NEXT_TUPLE**: Returns subsequent tuples from a complete cache entry
3. **MEMO_FILLING_CACHE**: Actively populating cache while returning tuples from the outer plan
4. **MEMO_CACHE_BYPASS_MODE**: Passes through tuples without caching when memory constraints are exceeded
5. **MEMO_END_OF_SCAN**: Terminal state indicating no more tuples available

The function maintains statistics on cache hits, misses, and overflows to monitor caching effectiveness. When memory pressure occurs, it gracefully degrades to bypass mode rather than failing.

## Parameters / Member Variables
- `pstate`: The plan state node containing the MemoizeState structure with caching information, hash table, and current execution state

## Dependencies
- Functions called/Symbols referenced:
  - ResetExprContext
  - build_hash_table
  - cache_lookup
  - cache_store_tuple
  - entry_purge_tuples
  - ExecProcNode
  - ExecStoreMinimalTuple
  - ExecCopySlot
  - outerPlanState
  - TupIsNull
- Called from (representative examples):
  - ExecInitMemoize (sets up the execution function pointer)

## Notes and Other Information
- Uses a hash table-based cache with configurable estimated entries from the planner
- Handles incomplete cache entries by purging and rebuilding rather than attempting partial recovery
- Includes special handling for single-row expectations to optimize cache completion marking
- Implements graceful degradation to bypass mode when cache storage fails due to memory constraints
- Maintains comprehensive execution statistics for query optimization feedback