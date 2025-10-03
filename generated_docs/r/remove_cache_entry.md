# remove_cache_entry

## Location
[src/backend/executor/nodeMemoize.c:374-401](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeMemoize.c#L374-L401)

## Overview
Removes a cache entry from the memoize cache and frees all memory associated with it, including the cached tuples and the entry structure itself.

## Definition

```c
static void
remove_cache_entry(MemoizeState *mstate, MemoizeEntry *entry)
```
## Detailed Description
This function is responsible for completely removing a cache entry from the PostgreSQL memoize cache system. It performs a comprehensive cleanup that includes removing the entry from the LRU (Least Recently Used) list, purging all cached tuples associated with the entry, updating memory accounting, removing the entry from the hash table, and freeing all allocated memory structures.

The function operates as part of the memoize node's cache management system, which is used to cache results of expensive subplan executions to improve query performance through result reuse.

## Parameters / Member Variables
- `*mstate`: Pointer to the MemoizeState structure containing the cache state and memory tracking information
- `*entry`: Pointer to the MemoizeEntry structure to be removed from the cache
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

## Simplified Source

```c
static void remove_cache_entry(MemoizeState *mstate, MemoizeEntry *entry) {
    MemoizeKey *key = entry->key;

    // Remove entry from LRU list
    dlist_delete(&entry->key->lru_node);

    // Remove all cached tuples from this entry
    entry_purge_tuples(mstate, entry);

    // Update memory accounting for the entry structure itself
    // (entry_purge_tuples already handled tuple memory)
    mstate->mem_used -= EMPTY_ENTRY_MEMORY_BYTES(entry);

    // Remove entry from the hash table
    memoize_delete_item(mstate->hashtable, entry);

    // Free the key's parameter data and the key itself
    pfree(key->params);
    pfree(key);
}
```