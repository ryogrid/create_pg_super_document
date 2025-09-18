# cache_purge_all

## Location
src/backend/executor/nodeMemoize.c: 402 - 439

## Overview
Removes all cached entries from the memoize cache by resetting the memory context, effectively clearing the entire cache and resetting all related data structures.

## Definition
```c
static void cache_purge_all(MemoizeState *mstate)
```

## Detailed Description
This function provides an efficient mechanism to completely clear the memoize cache by resetting the memory context that contains all cached data. Rather than iterating through each cache entry individually and freeing them one by one, this approach leverages PostgreSQL's memory context system to bulk-free all allocated memory in a single operation. The function also reinitializes all cache-related data structures to their empty state.

This approach is particularly efficient for large caches as it avoids the overhead of individual memory deallocations and hash table manipulations that would be required if entries were removed individually.

## Parameters / Member Variables
- `mstate`: Pointer to the MemoizeState structure containing the cache state that needs to be purged

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextReset (efficiently frees all memory in the table context)
  - dlist_init (reinitializes the LRU list)
- Types referenced:
  - MemoizeState
- Called from:
  - ExecReScanMemoize (during plan node rescanning operations)

## Notes and Other Information
- This is a static function, only accessible within nodeMemoize.c
- The function is highly efficient for bulk cache clearing as it uses memory context reset rather than individual entry removal
- All cache statistics are properly updated, with evictions count reflecting the number of entries that were purged
- After calling this function, the hash table is set to NULL and will be recreated on the next cache operation
- The LRU (Least Recently Used) list is reinitialized to empty state
- Memory usage tracking is reset to zero since all cached data has been freed
- Current tuple tracking pointers (last_tuple, entry) are reset to NULL to prevent dangling references