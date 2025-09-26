# cache_store_tuple

## Location
[src/backend/executor/nodeMemoize.c:625-696](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeMemoize.c#L625-L696)

## Overview
Adds a tuple from a TupleTableSlot to the current cache entry, managing memory allocation, linked list maintenance, and memory limit enforcement during the storage process.

## Definition
```c
static bool cache_store_tuple(MemoizeState *mstate, TupleTableSlot *slot)
```

## Detailed Description
This function stores individual tuples in the memoize cache by creating MemoizeTuple structures and linking them to the current cache entry. It handles the complete process of tuple storage including memory allocation, tuple copying, linked list management, memory accounting, and potential cache eviction if memory limits are exceeded.

The function assumes that a cache entry has already been established via cache_lookup() and that the mstate->last_tuple field correctly points to the tail of the entry's tuple list. It maintains the integrity of the tuple chain and handles the complexities of hash table reorganization that may occur during memory reduction operations.

## Parameters / Member Variables
- `mstate`: Pointer to the MemoizeState structure containing cache state and current entry information
- `slot`: Pointer to the TupleTableSlot containing the tuple data to be cached

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (allocates memory for new MemoizeTuple structure)
  - [ExecCopySlotMinimalTuple](../E/ExecCopySlotMinimalTuple.md) (creates a minimal tuple copy from the slot)
  - [cache_reduce_memory](cache_reduce_memory.md) (evicts entries if memory limit is exceeded)
  - [prepare_probe_slot](../p/prepare_probe_slot.md) (prepares hash lookup after eviction)
  - memoize_lookup (re-finds entry after potential hash table reorganization)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (manages memory context for allocations)
  - CACHE_TUPLE_BYTES (calculates memory usage for tuple storage)
  - Assert (debugging assertions)
- Types referenced:
  - [MemoizeState](../M/MemoizeState.md)
  - [TupleTableSlot](../T/TupleTableSlot.md)
  - [MemoizeTuple](../M/MemoizeTuple.md)
  - [MemoizeEntry](../M/MemoizeEntry.md)
  - [MemoizeKey](../M/MemoizeKey.md)
  - [MemoryContext](../M/MemoryContext.md)
- Called from:
  - [ExecMemoize](../E/ExecMemoize.md) (multiple locations during tuple processing)

## Notes and Other Information
- This is a static function, only accessible within nodeMemoize.c
- Returns false only if memory reduction fails to free sufficient space
- Properly maintains the linked list of cached tuples by updating both head and tail pointers
- Memory accounting is updated immediately when tuples are added
- Handles hash table reorganization that may occur during cache eviction by re-finding the entry
- Memory context switching ensures tuple allocation occurs in the correct context
- The function requires that cache_lookup() has been called previously to establish the current entry
- Maintains the invariant that mstate->last_tuple points to the tail of the tuple list
- Includes sophisticated error recovery for hash table element shuffling during memory reduction
- The tuple storage uses minimal tuple format for space efficiency
- Supports building cache entries incrementally by appending tuples one at a time