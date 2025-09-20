# DeleteRelationTuple

## Location
[src/backend/catalog/heap.c:1559-1587](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/heap.c#L1559-L1587)

## Overview
Removes the pg_class catalog entry for a specified relation during relation or index deletion operations.

## Definition

```c
void
DeleteRelationTuple(Oid relid)
```
## Detailed Description
This function is a focused utility that handles the removal of a relation's entry from the pg_class system catalog. It serves as a shared component in PostgreSQL's relation and index deletion workflows, providing a clean interface for removing the fundamental catalog entry that defines a relation's existence in the database.

The function performs a straightforward but critical operation:

1. **Catalog Access**: Opens the pg_class relation with RowExclusiveLock to ensure exclusive access during the deletion operation.

2. **Tuple Lookup**: Uses the system cache (RELOID cache) to efficiently locate the pg_class tuple corresponding to the given relation OID. This cache-based lookup is much faster than scanning the catalog directly.

3. **Validation**: Ensures the relation tuple exists in the catalog, reporting an error if the lookup fails, which would indicate an inconsistent database state.

4. **Deletion**: Removes the tuple from pg_class using CatalogTupleDelete, which handles both the physical removal and any associated index maintenance.

5. **Cleanup**: Properly releases the system cache entry and closes the pg_class relation.

This function is intentionally designed to be shared between relation deletion (heap_drop_with_catalog) and index deletion (index_drop) operations, as both types of objects have entries in pg_class that need to be removed when the object is dropped.

## Parameters / Member Variables
- : The OID of the relation whose pg_class entry should be deleted

## Dependencies
- Functions called/Symbols referenced:
  - table_open (opens pg_class catalog for modification)
  - [SearchSysCache1](../S/SearchSysCache1.md) (looks up relation tuple in RELOID cache)
  - HeapTupleIsValid (validates tuple lookup success)
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md) (deletes tuple from pg_class catalog)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (releases cached tuple)
  - table_close (closes pg_class catalog relation)
- Called from (representative examples):
  - [heap_drop_with_catalog](../h/heap_drop_with_catalog.md) (during table deletion)
  - [index_drop](../i/index_drop.md) (during index deletion)

## Notes and Other Information
- This function is explicitly documented as shared between relation and index deletion, and is not intended for use in other contexts
- Uses the system cache for efficient tuple lookup rather than scanning pg_class directly
- The RowExclusiveLock on pg_class ensures that no other transactions can modify the catalog during the deletion
- Error handling ensures that attempts to delete non-existent relations are caught and reported
- The function is part of the core catalog maintenance infrastructure and is called after other cleanup operations have been performed
- This operation is typically one of the final steps in relation deletion, after dependencies have been handled and other catalog entries have been cleaned up