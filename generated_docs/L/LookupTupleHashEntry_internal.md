# LookupTupleHashEntry_internal

## Location
[src/backend/executor/execGrouping.c:494-534](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execGrouping.c#L494-L534)

## Overview
Internal helper function that performs the core work of tuple hash table lookup and insertion operations, designed to minimize memory context switching overhead.

## Definition

```c
static inline TupleHashEntry
LookupTupleHashEntry_internal(TupleHashTable hashtable, TupleTableSlot *slot,
							  bool *isnew, uint32 hash)
```
## Detailed Description
This function serves as the internal implementation for both  and  functions. It handles the core logic of looking up or inserting tuples in a hash table while avoiding redundant memory context switches. The function can operate in two modes: when  is provided, it performs insertion (creating new entries if needed); when  is NULL, it performs lookup only. For new entries, it copies the tuple into the hash table's memory context and initializes the entry's additional data to NULL.

## Parameters / Member Variables
- : The tuple hash table to operate on
- : The tuple table slot containing the tuple to lookup or insert
- : Output parameter indicating whether a new entry was created (NULL for lookup-only mode)
- hash: hash table empty: Pre-computed hash value for the tuple

## Dependencies
- Functions called/Symbols referenced:
  - [TupleHashTable](../T/TupleHashTable.md)
  - [TupleHashEntryData](../T/TupleHashEntryData.md)
  - MinimalTuple
  - [ExecCopySlotMinimalTuple](../E/ExecCopySlotMinimalTuple.md)
  - tuplehash_insert_hash (via macro)
  - tuplehash_lookup_hash (via macro)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
- Called from (representative examples):
  - [LookupTupleHashEntry](LookupTupleHashEntry.md)
  - [LookupTupleHashEntryHash](LookupTupleHashEntryHash.md)

## Notes and Other Information
- This is a static inline function for performance optimization
- The function may change the memory context and expects the caller to restore it
- Uses a NULL key as a flag to reference the input slot directly
- The  parameter controls whether insertion or lookup-only operation is performed
- New entries have their  field initialized to NULL and  set to a copy of the input tuple
- Part of PostgreSQL's executor grouping functionality for hash-based operations like GROUP BY and DISTINCT

## Simplified Source

```c
static inline TupleHashEntry
LookupTupleHashEntry_internal(TupleHashTable hashtable, TupleTableSlot *slot,
                             bool *isnew, uint32 hash) {
    TupleHashEntryData *entry;
    bool found;
    MinimalTuple key = NULL;  // flag to reference inputslot

    if (isnew) {
        // Insert mode: create new entry if not found
        entry = tuplehash_insert_hash(hashtable->hashtab, key, hash, &found);

        if (found) {
            *isnew = false;  // found existing entry
        } else {
            *isnew = true;   // created new entry

            // Initialize new entry
            entry->additional = NULL;
            MemoryContextSwitchTo(hashtable->tablecxt);
            entry->firstTuple = ExecCopySlotMinimalTuple(slot);
        }
    } else {
        // Lookup-only mode
        entry = tuplehash_lookup_hash(hashtable->hashtab, key, hash);
    }

    return entry;
}
```