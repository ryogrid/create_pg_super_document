# CatalogTupleDelete

## Location
[src/backend/catalog/indexing.c:365-368](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/indexing.c#L365-L368)

## Overview
CatalogTupleDelete provides a uniform abstraction for deleting tuples from system catalog relations, currently implemented as a simple wrapper around heap deletion that maintains consistency with other catalog operations.

## Definition
```c
void CatalogTupleDelete(Relation heapRel, ItemPointer tid)
```

## Detailed Description
CatalogTupleDelete is designed to provide a consistent interface for catalog tuple deletion operations, abstracting away the underlying storage implementation details. Currently, it serves as a wrapper around simple_heap_delete since PostgreSQL heaps require no immediate index maintenance during deletion - cleanup is handled later by VACUUM.

The function is part of PostgreSQL's catalog management abstraction layer, designed to provide uniform interfaces for all catalog tuple changes (insert, update, delete). While currently trivial, this abstraction allows for potential future optimizations without requiring changes to numerous call sites throughout the system.

Unlike insert and update operations, there is no "WithInfo" version of this function because PostgreSQL's heap storage model requires no immediate index cleanup during deletion, making optimization through cached index state unnecessary.

## Parameters / Member Variables
- `heapRel`: The system catalog relation from which the tuple will be deleted
- `tid`: ItemPointer (tuple identifier) specifying the exact tuple to be deleted

## Dependencies
- Functions called/Symbols referenced:
  - [simple_heap_delete](../s/simple_heap_delete.md)
- Called from (representative examples):
  - [ExecGrant_Parameter](../E/ExecGrant_Parameter.md)
  - [DropObjectById](../D/DropObjectById.md)
  - [deleteOneObject](../d/deleteOneObject.md)
  - [DeleteInitPrivs](../D/DeleteInitPrivs.md)
  - [RelationRemoveInheritance](../R/RelationRemoveInheritance.md)
  - [DeleteRelationTuple](../D/DeleteRelationTuple.md)
  - [heap_drop_with_catalog](../h/heap_drop_with_catalog.md)
  - [RemoveStatistics](../R/RemoveStatistics.md)
  - [index_drop](../i/index_drop.md)
  - [deleteDependencyRecordsFor](../d/deleteDependencyRecordsFor.md)

## Notes and Other Information
- This function maintains the catalog management abstraction despite currently being a simple wrapper
- No immediate index maintenance is required due to PostgreSQL's heap storage architecture - VACUUM handles cleanup
- The abstraction is intentionally "leaky" as acknowledged in the code comments, since no WithInfo variant exists
- Widely used throughout PostgreSQL DDL operations for removing system catalog metadata
- Future caching of CatalogIndexState might eliminate the need for WithInfo variants across all catalog operations
- The uniform abstraction allows callers to not worry about storage-specific implementation details
- Essential for maintaining referential integrity when dropping database objects

## Simplified Source

```c
void CatalogTupleDelete(Relation heapRel, ItemPointer tid)
{
    // Simple wrapper around heap deletion
    // No immediate index work needed - VACUUM handles cleanup later
    simple_heap_delete(heapRel, tid);
}
```