# RemoveConstraintById

## Location
[src/backend/catalog/pg_constraint.c:612-702](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_constraint.c#L612-L702)

## Overview
Deletes a single constraint record from the system catalog by constraint OID, handling necessary cleanup and metadata updates for both relation and domain constraints.

## Definition

```c
void
RemoveConstraintById(Oid conId)
```
## Detailed Description
RemoveConstraintById is responsible for removing a constraint entry from the pg_constraint system catalog. The function performs different cleanup operations depending on the type of constraint being removed:

1. **Relation constraints**: Opens the relation with exclusive lock and updates metadata. For CHECK constraints, it decrements the relchecks count in pg_class to force relcache invalidation.
2. **Domain constraints**: Currently performs minimal processing (marked as TODO for future enhancement).

The function ensures proper locking semantics by holding AccessExclusiveLock on the target relation until transaction end, preventing concurrent modifications during constraint removal.

## Parameters / Member Variables
- : The OID of the constraint to be removed from the pg_constraint catalog

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - elog
  - GETSTRUCT
  - OidIsValid
  - SearchSysCacheCopy1
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md)
  - [ReleaseSysCache](ReleaseSysCache.md)
  - [table_close](../t/table_close.md)
- Called from (representative examples):
  - [doDeletion](../d/doDeletion.md) (dependency.c:1396)

## Notes and Other Information
- The function expects the constraint to exist; missing constraints trigger an ERROR
- For CHECK constraints on relations, the relchecks counter in pg_class is decremented to trigger relcache rebuilding
- Domain constraint removal is currently minimal and marked for future enhancement
- Maintains proper lock hierarchy: AccessExclusiveLock on target relation, RowExclusiveLock on catalogs
- The constraint's relation lock is held until transaction end to ensure consistency