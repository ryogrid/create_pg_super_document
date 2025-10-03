# heap_freetuple

## Location
[src/backend/access/common/heaptuple.c:1434-1451](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/heaptuple.c#L1434-L1451)

## Overview
Frees the memory allocated for a HeapTuple structure by calling pfree() on the tuple pointer.

## Definition

```c
void
heap_freetuple(HeapTuple htup)
```
## Detailed Description
The  function is a simple wrapper around PostgreSQL's memory management system that deallocates the memory occupied by a HeapTuple. This function is essential for preventing memory leaks when heap tuples are no longer needed. It simply calls  on the provided HeapTuple pointer, which releases the memory back to PostgreSQL's memory context system.

This function is used extensively throughout the PostgreSQL codebase whenever heap tuples need to be cleaned up after processing, whether in storage operations, catalog maintenance, replication, or general tuple manipulation.

## Parameters / Member Variables
- `htup`: A pointer to the HeapTuple structure to be freed. The tuple must have been previously allocated through PostgreSQL's memory management system.
## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [toast_save_datum](../t/toast_save_datum.md)
  - [heap_insert](heap_insert.md)
  - [heap_delete](heap_delete.md)
  - [heap_update](heap_update.md)
  - [ExtractReplicaIdentity](../E/ExtractReplicaIdentity.md)
  - [reform_and_rewrite_tuple](../r/reform_and_rewrite_tuple.md)
  - [rewrite_heap_tuple](../r/rewrite_heap_tuple.md)
  - [InsertOneTuple](../I/InsertOneTuple.md)
  - [CatalogTuplesMultiInsertWithInfo](../C/CatalogTuplesMultiInsertWithInfo.md)
  - [ExecBRInsertTriggers](../E/ExecBRInsertTriggers.md)
  - [SPI_freetuple](../S/SPI_freetuple.md)
  - [RelationBuildDesc](../R/RelationBuildDesc.md)

## Notes and Other Information
- This is a fundamental memory management function used throughout PostgreSQL
- The function assumes the HeapTuple was allocated using PostgreSQL's memory context system
- It's critical to call this function to prevent memory leaks when heap tuples are no longer needed
- The function is used in both normal database operations and system catalog maintenance
- Used extensively in trigger execution, replication, and storage layer operations

## Simplified Source

```c
// Simplified version of heap_freetuple
void heap_freetuple(HeapTuple htup) {
    // Free the memory allocated for the heap tuple
    // This releases the tuple back to PostgreSQL's memory context system
    pfree(htup);
}
```

Key simplifications made:
- Added explanatory comments for clarity
- The function is already at its simplest form - it's a direct wrapper around pfree()
- No simplification of logic was needed as the function contains only one operation
- Preserved the essential memory deallocation functionality