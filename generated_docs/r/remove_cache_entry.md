# remove_cache_entry

## Location
src/backend/executor/nodeMemoize.c: 374 - 401

## Overview
Removes a cache entry from the memoize cache and frees all memory associated with it, including the cached tuples and the entry structure itself.

## Definition


## Detailed Description
This function is responsible for completely removing a cache entry from the PostgreSQL memoize cache system. It performs a comprehensive cleanup that includes removing the entry from the LRU (Least Recently Used) list, purging all cached tuples associated with the entry, updating memory accounting, removing the entry from the hash table, and freeing all allocated memory structures.

The function operates as part of the memoize node's cache management system, which is used to cache results of expensive subplan executions to improve query performance through result reuse.

## Parameters / Member Variables
- : Pointer to the MemoizeState structure containing the cache state and memory tracking information
- : Pointer to the MemoizeEntry structure to be removed from the cache

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_delete](../d/dlist_delete.md) (removes entry from LRU list)
  - [entry_purge_tuples](../e/entry_purge_tuples.md) (removes all cached tuples from the entry)
  - memoize_delete_item (removes entry from hash table)
  - [pfree](../p/pfree.md) (frees allocated memory)
  - EMPTY_ENTRY_MEMORY_BYTES (macro for calculating entry memory usage)
- Types referenced:
  - [MemoizeState](../M/MemoizeState.md)
  - [MemoizeEntry](../M/MemoizeEntry.md)  
  - [MemoizeKey](../M/MemoizeKey.md)
- Called from:
  - [cache_reduce_memory](../c/cache_reduce_memory.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the nodeMemoize.c file
- Memory accounting is carefully maintained by subtracting the memory used by the entry structure
- The function assumes that entry_purge_tuples has already handled memory accounting for the cached tuples
- The LRU node removal ensures proper cache eviction ordering is maintained
- All memory allocated for the key parameters and key structure is properly freed to prevent memory leaks