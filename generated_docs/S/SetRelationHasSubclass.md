# SetRelationHasSubclass

## Location
[src/backend/commands/tablecmds.c:3515-3560](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L3515-L3560)

## Overview
SetRelationHasSubclass updates the relhassubclass field in pg_class system catalog to indicate whether a relation has child tables, triggering query plan invalidation for inheritance optimization.

## Definition

```c
void
SetRelationHasSubclass(Oid relationId, bool relhassubclass)
```
## Detailed Description
This function is critical for maintaining accurate inheritance metadata in PostgreSQL's system catalogs. It updates the relhassubclass field in the pg_class catalog, which is used by the query planner to determine if a relation has child tables that need to be considered during query execution.

Key aspects of the function:
- **Safe Operation**: Setting to true is always safe since SQL commands can handle finding no children after expecting them
- **Locking Requirements**: Requires exclusive locking to prevent race conditions when setting to false
- **Cache Invalidation**: Always triggers SI (Shared Invalidation) messages to all backends, forcing query plan rebuilds even when no catalog change occurs
- **Tuple Management**: Handles pg_class tuple retrieval, modification, and cleanup using proper PostgreSQL catalog APIs

The SI invalidation is crucial because query plans cache information about inheritance hierarchies, and this ensures all backends see the updated inheritance status.

## Parameters / Member Variables
- : OID of the relation whose relhassubclass field should be updated
- : Boolean value indicating whether the relation has subclasses/child tables

## Dependencies
- Functions called/Symbols referenced:
  - [CheckRelationOidLockedByMe](../C/CheckRelationOidLockedByMe.md)
  - table_open
  - table_close
  - SearchSysCacheCopy1
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [CacheInvalidateRelcacheByTuple](../C/CacheInvalidateRelcacheByTuple.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - Form_pg_class (structure type)
- Called from (representative examples):
  - [StoreCatalogInheritance1](StoreCatalogInheritance1.md)
  - index_create
  - [IndexSetParentIndex](../I/IndexSetParentIndex.md)

## Notes and Other Information
- Requires caller to hold ShareUpdateExclusiveLock or ShareRowExclusiveLock until transaction end
- When setting to false, caller must acquire lock before reading evidence that justifies the false value
- Always triggers relcache rebuilds via SI invalidation messages, even when tuple doesn't change
- Performs assertion checks to verify proper locking
- Uses SearchSysCacheCopy1 to get a modifiable tuple copy
- Handles both cases: when catalog update is needed and when only cache invalidation is required
- Essential for query planner optimization of inheritance hierarchies