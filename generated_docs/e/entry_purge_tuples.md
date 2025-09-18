# entry_purge_tuples

## Location
src/backend/executor/nodeMemoize.c: 344 - 373

## Overview
Removes all cached tuples from a MemoizeEntry while updating memory accounting, leaving an empty but valid cache entry.

## Definition
```c
static inline void entry_purge_tuples(MemoizeState *mstate, MemoizeEntry *entry)
```

## Detailed Description
This function purges all tuples from a specific cache entry by traversing the linked list of MemoizeTuple structures and freeing their memory. It performs the following operations:

1. **Traverse tuple list**: Iterates through the linked list starting from entry->tuplehead
2. **Memory accounting**: Accumulates freed memory using CACHE_TUPLE_BYTES() macro
3. **Memory cleanup**: Frees both the MinimalTuple data and the MemoizeTuple wrapper structure
4. **Entry reset**: Sets entry->complete to false and entry->tuplehead to NULL
5. **Update accounting**: Decrements mstate->mem_used by the total freed memory

After completion, the entry remains in the cache but contains no tuples and is marked as incomplete.

## Parameters / Member Variables
- `mstate`: Pointer to MemoizeState for accessing memory usage accounting
- `entry`: Pointer to MemoizeEntry whose tuples should be purged

## Dependencies
- Functions called/Symbols referenced:
  - MemoizeEntry
  - MemoizeState
  - MemoizeTuple
  - CACHE_TUPLE_BYTES
- Called from (representative examples):
  - remove_cache_entry
  - ExecMemoize

## Notes and Other Information
- The inline keyword indicates this is a performance-critical function
- Safely handles empty entries (NULL tuplehead) without issues
- Updates memory accounting to maintain accurate cache size tracking
- Entry remains in the hash table but is marked as incomplete and empty
- Both the MinimalTuple data and MemoizeTuple wrapper are freed to prevent memory leaks
- The freed_mem accumulation ensures accurate memory usage reporting