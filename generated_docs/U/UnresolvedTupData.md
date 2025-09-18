# UnresolvedTupData

## Location
src/backend/access/heap/rewriteheap.c: 175 - 176

## Overview
UnresolvedTupData is a structure used during heap rewriting operations to track tuples that have unresolved ctid (current tuple identifier) references, helping maintain update chains when tuples are being moved to a new heap.

## Definition


## Detailed Description
UnresolvedTupData is a core component of PostgreSQL's heap rewriting facility, specifically used to handle the complex problem of maintaining ctid chains during table rewrites. When a table is being rewritten (such as during ALTER TABLE operations), tuples must be copied to a new heap while preserving the visibility information and update chains that link different versions of the same logical row.

The structure is used in the unresolved_tups hash table to temporarily store tuples (A) that reference other tuples (B) via ctid, but where the referenced tuple B hasn't been encountered yet during the rewrite process. Once tuple B is found and its new location is determined, tuple A can be updated with the correct ctid and written to the new heap.

This mechanism ensures that the complex web of tuple version chains remains intact even when all tuples are being physically relocated during a heap rewrite operation.

## Parameters / Member Variables
- : A TidHashKey structure containing the expected xmin (transaction ID) and old location of the tuple B that this tuple A is supposed to reference
- : An ItemPointerData structure storing the original location of tuple A in the old heap before the rewrite
- : A HeapTuple containing the actual tuple contents of tuple A that will eventually be written to the new heap

## Dependencies
- Functions called/Symbols referenced:
  - TidHashKey (hash key structure)
  - ItemPointerData (tuple identifier structure)
  - HeapTuple (tuple data structure)
- Called from (representative examples):
  - begin_heap_rewrite (where unresolved tuple tracking is initialized)

## Notes and Other Information
- This structure is specifically designed for heap rewrite operations and is not used in normal tuple processing
- The structure is part of a sophisticated mechanism to handle the A->B ctid reference problem during rewrites
- It works in conjunction with OldToNewMappingData to maintain referential integrity during heap rewrites
- The structure is allocated and managed through hash tables during the rewrite process
- Critical for maintaining MVCC (Multi-Version Concurrency Control) semantics during table restructuring operations