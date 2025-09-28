# simple_heap_insert

## Location
[src/backend/access/heap/heapam.c:2673-2685](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L2673-L2685)

## Overview
simple_heap_insert is a simplified wrapper around heap_insert() that provides default parameters for basic tuple insertion, primarily used for system catalog modifications.

## Definition

```c
void
simple_heap_insert(Relation relation, HeapTuple tup)
```
## Detailed Description
This function serves as a convenience wrapper for heap_insert() with default parameters. It automatically supplies the current command ID using GetCurrentCommandId(true) and sets options to 0 with no bulk insert state. The function is designed for straightforward tuple insertions where the caller doesn't need to specify custom insertion options or provide bulk insert optimization state. It's the recommended interface for most system catalog operations where simplicity is preferred over fine-grained control.

## Parameters / Member Variables
- : The target heap relation where the tuple will be inserted
- : The HeapTuple to be inserted into the relation

## Dependencies
- Functions called/Symbols referenced:
  - [heap_insert](../h/heap_insert.md)
  - [GetCurrentCommandId](../G/GetCurrentCommandId.md)
- Called from:
  - [InsertOneTuple](../I/InsertOneTuple.md) (bootstrap.c)
  - [CatalogTupleInsert](../C/CatalogTupleInsert.md) (indexing.c)
  - [CatalogTupleInsertWithInfo](../C/CatalogTupleInsertWithInfo.md) (indexing.c)

## Notes and Other Information
- This is the preferred function for system catalog modifications
- Provides no access to speedup options or bulk insert state that heap_insert offers
- Uses GetCurrentCommandId(true) to automatically obtain the current command ID
- Sets insertion options to 0 (no special flags)
- Passes NULL for bulk insert state parameter
- More suitable for single-tuple insertions in system catalogs than heap_insert

## Simplified Source

```c
// Simplified version of simple_heap_insert
void simple_heap_insert(Relation relation, HeapTuple tup) {
    // Call heap_insert with default parameters for system catalog operations
    heap_insert(relation, tup, GetCurrentCommandId(true), 0, NULL);
}
```

Key simplifications made:
- Core logic: wrapper that calls heap_insert with sensible defaults
- Automatically uses current command ID for transaction management
- No special insertion options or bulk insert optimizations
- Preferred interface for straightforward system catalog modifications