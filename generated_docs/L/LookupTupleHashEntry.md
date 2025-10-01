# LookupTupleHashEntry

## Location
[src/backend/executor/execGrouping.c:304-335](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execGrouping.c#L304-L335)

## Overview
Finds or creates a hashtable entry for the tuple group containing the given tuple, serving as the primary interface for tuple hash table lookups in PostgreSQL's execution engine.

## Definition

```c
TupleHashEntry
LookupTupleHashEntry(TupleHashTable hashtable, TupleTableSlot *slot,
					 bool *isnew, uint32 *hash)
```
## Detailed Description
LookupTupleHashEntry is the main function for interacting with tuple hash tables in PostgreSQL's executor. It provides a unified interface for both lookup and insertion operations. The function computes a hash value for the input tuple and either finds an existing matching entry or creates a new one based on the caller's requirements.

The function operates in two modes:
1. **Lookup-only mode**: When  is NULL, only searches for existing entries without creating new ones
2. **Lookup-or-insert mode**: When  is provided, creates a new entry if no match is found

The function handles memory context switching to ensure hash computations occur in the appropriate short-lived context, preventing memory leaks during hash table operations.

## Parameters / Member Variables
- : The TupleHashTable to search in or add to
- : TupleTableSlot containing the tuple to look up or insert
- : Pointer to bool indicating whether to create new entries; if NULL, no new entries are created; on return, set to true if entry was newly created
- hash: hash table empty: Optional pointer to receive the computed hash value for the tuple

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [TupleHashTableHash_internal](../T/TupleHashTableHash_internal.md)
  - [LookupTupleHashEntry_internal](LookupTupleHashEntry_internal.md)
- Called from (representative examples):
  - [lookup_hash_entries](../l/lookup_hash_entries.md) (nodeAgg.c:2119)
  - [ExecRecursiveUnion](../E/ExecRecursiveUnion.md) (nodeRecursiveunion.c:97, 144)
  - [setop_fill_hash_table](../s/setop_fill_hash_table.md) (nodeSetOp.c:383, 404)
  - [buildSubPlanHash](../b/buildSubPlanHash.md) (nodeSubplan.c:632, 637)

## Notes and Other Information
- Always switches to hashtable->tempcxt for hash function execution to ensure proper memory management
- Returns NULL when no match is found and isnew is NULL (lookup-only mode)
- For new entries, the additional_data field is automatically zeroed
- The function validates that returned entries have the correct hash value via assertion
- Critical for grouping operations in aggregation, set operations, and subplan execution

## Simplified Source

```c
TupleHashEntry
LookupTupleHashEntry(TupleHashTable hashtable, TupleTableSlot *slot,
                     bool *isnew, uint32 *hash)
{
    TupleHashEntry entry;
    MemoryContext oldContext;
    uint32 local_hash;

    // Switch to temporary context for hash computation
    oldContext = MemoryContextSwitchTo(hashtable->tempcxt);

    // Set up hashtable state for hash and equality functions
    hashtable->inputslot = slot;
    hashtable->in_hash_funcs = hashtable->tab_hash_funcs;
    hashtable->cur_eq_func = hashtable->tab_eq_func;

    // Compute hash value for the tuple
    local_hash = TupleHashTableHash_internal(hashtable->hashtab, NULL);

    // Look up or insert the entry
    entry = LookupTupleHashEntry_internal(hashtable, slot, isnew, local_hash);

    // Return hash value to caller if requested
    if (hash != NULL) {
        *hash = local_hash;
    }

    // Restore original memory context
    MemoryContextSwitchTo(oldContext);

    return entry;
}
```