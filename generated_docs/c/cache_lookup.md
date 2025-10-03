# cache_lookup

## Location
[src/backend/executor/nodeMemoize.c:528-624](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeMemoize.c#L528-L624)

## Overview
Searches for existing cache entries based on current scan parameters, creates new entries if not found, and manages LRU ordering and memory limits during the lookup process.

## Definition
```c
static MemoizeEntry *cache_lookup(MemoizeState *mstate, bool *found)
```

## Detailed Description
This function is the core cache lookup mechanism for the PostgreSQL memoize node. It performs a hash table lookup for cached results based on the current scan parameters. If an existing entry is found, it updates the LRU ordering and returns the entry. If no entry exists, it creates a new empty entry, initializes it, adds it to the LRU list, and manages memory limits by potentially evicting older entries.

The function handles the complexity of hash table management, including dealing with potential hash table reorganization that can occur during memory reduction operations. It ensures cache consistency and proper memory accounting throughout the lookup process.

## Parameters / Member Variables
- `mstate`: Pointer to the MemoizeState structure containing cache state and configuration
- `found`: Output parameter that indicates whether an existing cache entry was found (true) or a new entry was created (false)

## Dependencies
- Functions called/Symbols referenced:
  - [prepare_probe_slot](../p/prepare_probe_slot.md) (sets up hash table lookup parameters)
  - memoize_insert (inserts/finds entries in hash table)
  - [dlist_move_tail](../d/dlist_move_tail.md) (moves existing entry to end of LRU list)
  - [palloc](../p/palloc.md) (allocates memory for new key)
  - [ExecCopySlotMinimalTuple](../E/ExecCopySlotMinimalTuple.md) (copies scan parameters as minimal tuple)
  - [dlist_push_tail](../d/dlist_push_tail.md) (adds new entry to end of LRU list)
  - [cache_reduce_memory](cache_reduce_memory.md) (evicts entries if over memory limit)
  - memoize_lookup (re-finds entry after hash table reorganization)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (manages memory context)
  - EMPTY_ENTRY_MEMORY_BYTES (calculates entry memory usage)
- Types referenced:
  - [MemoizeState](../M/MemoizeState.md)
  - [MemoizeEntry](../M/MemoizeEntry.md)
  - [MemoizeKey](../M/MemoizeKey.md)
  - [MemoryContext](../M/MemoryContext.md)
- Called from:
  - [ExecMemoize](../E/ExecMemoize.md) (main executor function for memoize nodes)

## Notes and Other Information
- This is a static function, only accessible within nodeMemoize.c
- Returns NULL only when memory reduction fails, which is highly unlikely since new entries contain no tuples yet
- Properly manages LRU ordering by moving found entries to the tail (most recently used position)
- New entries are initialized with complete=false and tuplehead=NULL
- Handles hash table reorganization that may occur during memory reduction by re-finding the entry
- Memory context switching ensures new key allocation happens in the correct context
- The function guarantees that when *found is true, the return value is never NULL
- Includes sophisticated handling for hash table element shuffling during eviction operations
- Memory accounting is updated immediately when new entries are created
- The probeslot mechanism is used for efficient parameter-based lookups without creating temporary keys

## Simplified Source

```c
static MemoizeEntry *
cache_lookup(MemoizeState *mstate, bool *found)
{
    MemoizeKey *key;
    MemoizeEntry *entry;
    MemoryContext oldcontext;

    // Set up probe slot with current scan parameters
    prepare_probe_slot(mstate, NULL);

    // Try to find existing entry or create new one
    entry = memoize_insert(mstate->hashtable, NULL, found);

    if (*found) {
        // Move existing entry to end of LRU list (most recently used)
        dlist_move_tail(&mstate->lru_list, &entry->key->lru_node);
        return entry;
    }

    // Create new entry
    oldcontext = MemoryContextSwitchTo(mstate->tableContext);

    // Allocate and initialize new key
    entry->key = key = (MemoizeKey *) palloc(sizeof(MemoizeKey));
    key->params = ExecCopySlotMinimalTuple(mstate->probeslot);

    // Update memory usage and initialize entry
    mstate->mem_used += EMPTY_ENTRY_MEMORY_BYTES(entry);
    entry->complete = false;
    entry->tuplehead = NULL;

    // Add to end of LRU list
    dlist_push_tail(&mstate->lru_list, &entry->key->lru_node);
    mstate->last_tuple = NULL;

    MemoryContextSwitchTo(oldcontext);

    // Handle memory limit by evicting old entries if needed
    if (mstate->mem_used > mstate->mem_limit) {
        if (unlikely(!cache_reduce_memory(mstate, key)))
            return NULL;

        // Hash table may have been reorganized, re-find entry if needed
        if (entry->status != memoize_SH_IN_USE || entry->key != key) {
            prepare_probe_slot(mstate, key);
            entry = memoize_lookup(mstate->hashtable, NULL);
            Assert(entry != NULL);
        }
    }

    return entry;
}
```