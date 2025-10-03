# LookupTupleHashEntryHash

## Location
[src/backend/executor/execGrouping.c:359-390](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execGrouping.c#L359-L390)

## Overview
A variant of LookupTupleHashEntry for callers that have already computed the hash value, providing optimized hash table lookups when the hash is pre-calculated.

## Definition
```c
TupleHashEntry LookupTupleHashEntryHash(TupleHashTable hashtable, TupleTableSlot *slot, bool *isnew, uint32 hash)
```

## Detailed Description
LookupTupleHashEntryHash is an optimized version of LookupTupleHashEntry designed for scenarios where the caller has already computed the hash value for the tuple. This function skips the hash calculation step and directly proceeds to the lookup or insertion operation using the provided hash value.

Like its counterpart LookupTupleHashEntry, this function supports both lookup-only and lookup-or-insert modes based on the isnew parameter. It provides the same functionality but with improved performance when the hash value is already available, avoiding redundant hash computations.

The function is particularly useful in scenarios where hash values are computed once and used multiple times, or where custom hash calculation logic is employed before the hash table operation.

## Parameters / Member Variables
- `hashtable`: The TupleHashTable to search in or add to
- `slot`: TupleTableSlot containing the tuple to look up or insert
- `isnew`: Pointer to bool indicating whether to create new entries; if NULL, no new entries are created; on return, set to true if entry was newly created
- `hash`: Pre-computed hash value for the tuple

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [LookupTupleHashEntry_internal](LookupTupleHashEntry_internal.md)
- Called from (representative examples):
  - [agg_refill_hash_table](../a/agg_refill_hash_table.md) (nodeAgg.c:2677)
  - Referenced in executor.h header for external usage

## Notes and Other Information
- Avoids redundant hash computation when the hash value is already known
- Maintains the same memory context switching behavior as LookupTupleHashEntry
- Validates that returned entries have the correct hash value via assertion
- Useful for performance optimization in aggregation operations where hash values may be cached or computed separately
- Part of the tuple hash table API that provides flexibility for different usage patterns
- For new entries, the additional_data field is automatically zeroed by the internal implementation

## Simplified Source

```c
TupleHashEntry LookupTupleHashEntryHash(TupleHashTable hashtable, TupleTableSlot *slot,
                                       bool *isnew, uint32 hash) {
    TupleHashEntry entry;
    MemoryContext oldContext;

    // Switch to temporary context for hash operations
    oldContext = MemoryContextSwitchTo(hashtable->tempcxt);

    // Set up hash table context with slot and functions
    hashtable->inputslot = slot;
    hashtable->in_hash_funcs = hashtable->tab_hash_funcs;
    hashtable->cur_eq_func = hashtable->tab_eq_func;

    // Perform the actual lookup/insert with provided hash
    entry = LookupTupleHashEntry_internal(hashtable, slot, isnew, hash);

    // Validate hash consistency
    Assert(entry == NULL || entry->hash == hash);

    // Restore previous memory context
    MemoryContextSwitchTo(oldContext);

    return entry;
}
```