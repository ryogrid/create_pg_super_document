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
  - [table_open](../t/table_open.md)
  - [table_close](../t/table_close.md)
  - SearchSysCacheCopy1
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - ObjectAddressSet
  - [recordDependencyOn](../r/recordDependencyOn.md)
  - [deleteDependencyRecordsForClass](../d/deleteDependencyRecordsForClass.md)
  - [changeDependencyFor](../c/changeDependencyFor.md)
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md)
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

## Simplified Source

```c
static void
ATExecSetAccessMethodNoStorage(Relation rel, Oid newAccessMethodId)
{
    Relation pg_class;
    Oid oldAccessMethodId;
    HeapTuple tuple;
    Form_pg_class rd_rel;
    Oid reloid = RelationGetRelid(rel);

    // This function should only be called for relations without storage
    Assert(!RELKIND_HAS_STORAGE(rel->rd_rel->relkind));

    // Open pg_class catalog for updates
    pg_class = table_open(RelationRelationId, RowExclusiveLock);

    // Get the relation's catalog entry
    tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(reloid));
    if (!HeapTupleIsValid(tuple))
        elog(ERROR, "cache lookup failed for relation %u", reloid);

    rd_rel = (Form_pg_class) GETSTRUCT(tuple);
    oldAccessMethodId = rd_rel->relam;

    // Update the access method
    rd_rel->relam = newAccessMethodId;

    // Skip if no change needed
    if (rd_rel->relam == oldAccessMethodId)
    {
        heap_freetuple(tuple);
        table_close(pg_class, RowExclusiveLock);
        return;
    }

    // Update the catalog
    CatalogTupleUpdate(pg_class, &tuple->t_self, tuple);

    // Manage access method dependencies
    if (!OidIsValid(oldAccessMethodId) && OidIsValid(rd_rel->relam))
    {
        // Add new dependency
        ObjectAddress relobj, referenced;
        ObjectAddressSet(relobj, RelationRelationId, reloid);
        ObjectAddressSet(referenced, AccessMethodRelationId, rd_rel->relam);
        recordDependencyOn(&relobj, &referenced, DEPENDENCY_NORMAL);
    }
    else if (OidIsValid(oldAccessMethodId) && !OidIsValid(rd_rel->relam))
    {
        // Remove existing dependency
        deleteDependencyRecordsForClass(RelationRelationId, reloid,
                                       AccessMethodRelationId, DEPENDENCY_NORMAL);
    }
    else
    {
        // Update existing dependency
        Assert(OidIsValid(oldAccessMethodId) && OidIsValid(rd_rel->relam));
        changeDependencyFor(RelationRelationId, reloid, AccessMethodRelationId,
                           oldAccessMethodId, rd_rel->relam);
    }

    // Make changes visible and trigger hooks
    CommandCounterIncrement();
    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), 0);

    // Cleanup
    heap_freetuple(tuple);
    table_close(pg_class, RowExclusiveLock);
}
```