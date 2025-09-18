# ATExecSetAccessMethodNoStorage

## Location
[src/backend/commands/tablecmds.c:14929-15018](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L14929-L15018)

## Overview
Executes ALTER TABLE SET ACCESS METHOD for relations with no storage by updating only the catalog metadata without requiring table rewriting.

## Definition
```c
static void ATExecSetAccessMethodNoStorage(Relation rel, Oid newAccessMethodId)
```

## Detailed Description
This function handles the execution phase of ALTER TABLE SET ACCESS METHOD for relations that have no physical storage (such as partitioned tables, views, etc.) but still need to track their access method in the system catalog. Since these relations don't store actual data, changing their access method is purely a catalog operation that doesn't require rewriting any data.

The function updates the pg_class.relam field for the relation and manages the dependency relationships between the relation and the access method. It handles three scenarios: (1) adding a new dependency when transitioning from no access method to a specific one, (2) removing the dependency when changing to InvalidOid, and (3) updating an existing dependency when changing from one access method to another. The function ensures proper catalog consistency by using appropriate locking and making changes visible through CommandCounterIncrement.

## Parameters / Member Variables
- `rel`: The relation whose access method is being changed
- `newAccessMethodId`: The OID of the new access method, or InvalidOid to remove access method designation

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - table_close
  - SearchSysCacheCopy1
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - ObjectAddressSet
  - [recordDependencyOn](../r/recordDependencyOn.md)
  - [deleteDependencyRecordsForClass](../d/deleteDependencyRecordsForClass.md)
  - [changeDependencyFor](../c/changeDependencyFor.md)
  - CommandCounterIncrement
  - InvokeObjectPostAlterHook
  - RelationGetRelid
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md)

## Notes and Other Information
- This is a static function only accessible within tablecmds.c as part of the ALTER TABLE infrastructure
- Only handles relations without storage (checked via RELKIND_HAS_STORAGE assertion)
- Manages dependency tracking between relations and access methods in pg_depend
- Uses RowExclusiveLock on pg_class to ensure exclusive access during catalog updates
- Invokes post-alter hooks to notify other subsystems of the change
- Part of PostgreSQL's ALTER TABLE execution phase for access method changes
- Located in src/backend/commands/tablecmds.c:14929-15018