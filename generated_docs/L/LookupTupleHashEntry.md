# LookupTupleHashEntry

## Location
src/backend/executor/execGrouping.c: 304 - 335

## Overview
Finds or creates a hashtable entry for the tuple group containing the given tuple, serving as the primary interface for tuple hash table lookups in PostgreSQL's execution engine.

## Definition


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
  - MemoryContextSwitchTo
  - TupleHashTableHash_internal
  - LookupTupleHashEntry_internal
- Called from (representative examples):
  - lookup_hash_entries (nodeAgg.c:2119)
  - ExecRecursiveUnion (nodeRecursiveunion.c:97, 144)
  - setop_fill_hash_table (nodeSetOp.c:383, 404)
  - buildSubPlanHash (nodeSubplan.c:632, 637)

## Notes and Other Information
- Always switches to hashtable->tempcxt for hash function execution to ensure proper memory management
- Returns NULL when no match is found and isnew is NULL (lookup-only mode)
- For new entries, the additional_data field is automatically zeroed
- The function validates that returned entries have the correct hash value via assertion
- Critical for grouping operations in aggregation, set operations, and subplan execution