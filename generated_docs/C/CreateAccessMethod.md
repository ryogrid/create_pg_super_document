# CreateAccessMethod

## Location
[src/backend/commands/amcmds.c:43-128](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/amcmds.c#L43-L128)

## Overview
Registers a new access method in the PostgreSQL system catalog, creating the necessary catalog entries and dependency records.

## Definition

```c
ObjectAddress
CreateAccessMethod(CreateAmStmt *stmt)
```
## Detailed Description
CreateAccessMethod processes a CREATE ACCESS METHOD statement by inserting a new tuple into the pg_am system catalog. The function performs several validation checks including superuser privilege verification and name uniqueness, then creates the catalog entry with proper dependency tracking. It establishes a dependency relationship between the access method and its handler function, and records the access method as part of the current extension if applicable.

## Parameters / Member Variables
- : Pointer to CreateAmStmt structure containing the access method name, handler function name, and access method type

## Dependencies
- Functions called/Symbols referenced:
  - [superuser](../s/superuser.md): Checks if current user has superuser privileges
  - GetSysCacheOid1: Looks up existing access method by name
  - [lookup_am_handler_func](../l/lookup_am_handler_func.md): Validates and retrieves handler function OID
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md): Generates new OID for the access method
  - [heap_form_tuple](../h/heap_form_tuple.md): Creates heap tuple for catalog insertion
  - [CatalogTupleInsert](CatalogTupleInsert.md): Inserts tuple into pg_am catalog
  - [heap_freetuple](../h/heap_freetuple.md): Frees tuple memory
  - [recordDependencyOn](../r/recordDependencyOn.md): Records dependency on handler function
  - [recordDependencyOnCurrentExtension](../r/recordDependencyOnCurrentExtension.md): Records extension membership
  - InvokeObjectPostCreateHook: Triggers post-creation hooks
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md): Main utility command processor

## Notes and Other Information
- Requires superuser privileges to execute
- Validates that the access method name is unique in the system
- Automatically establishes DEPENDENCY_NORMAL relationship with the handler function
- Supports extension membership tracking for proper cleanup during extension drops
- Uses row-exclusive locking on the pg_am catalog during the operation
- Location: src/backend/commands/amcmds.c:43-128

## Simplified Source

```c
ObjectAddress CreateAccessMethod(CreateAmStmt *stmt)
{
    Relation    rel;
    ObjectAddress myself;
    ObjectAddress referenced;
    Oid         amoid;
    Oid         amhandler;
    bool        nulls[Natts_pg_am];
    Datum       values[Natts_pg_am];
    HeapTuple   tup;

    // Open the access method catalog
    rel = table_open(AccessMethodRelationId, RowExclusiveLock);

    // Only superusers can create access methods
    if (!superuser())
        ereport(ERROR, "permission denied to create access method - must be superuser");

    // Check if the access method name already exists
    amoid = GetSysCacheOid1(AMNAME, Anum_pg_am_oid, CStringGetDatum(stmt->amname));
    if (OidIsValid(amoid))
        ereport(ERROR, "access method already exists");

    // Validate and get the handler function OID
    amhandler = lookup_am_handler_func(stmt->handler_name, stmt->amtype);

    // Prepare the new tuple data
    memset(values, 0, sizeof(values));
    memset(nulls, false, sizeof(nulls));

    // Get a new OID for this access method
    amoid = GetNewOidWithIndex(rel, AmOidIndexId, Anum_pg_am_oid);

    // Set up the column values
    values[Anum_pg_am_oid - 1] = ObjectIdGetDatum(amoid);
    values[Anum_pg_am_amname - 1] = DirectFunctionCall1(namein, CStringGetDatum(stmt->amname));
    values[Anum_pg_am_amhandler - 1] = ObjectIdGetDatum(amhandler);
    values[Anum_pg_am_amtype - 1] = CharGetDatum(stmt->amtype);

    // Create and insert the new tuple
    tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);
    CatalogTupleInsert(rel, tup);
    heap_freetuple(tup);

    // Set up object address for dependency tracking
    myself.classId = AccessMethodRelationId;
    myself.objectId = amoid;
    myself.objectSubId = 0;

    // Record dependency on the handler function
    referenced.classId = ProcedureRelationId;
    referenced.objectId = amhandler;
    referenced.objectSubId = 0;
    recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

    // Record as part of current extension if applicable
    recordDependencyOnCurrentExtension(&myself, false);

    // Trigger post-creation hooks
    InvokeObjectPostCreateHook(AccessMethodRelationId, amoid, 0);

    // Clean up and return
    table_close(rel, RowExclusiveLock);
    return myself;
}
```