# CreateForeignDataWrapper

## Location
[src/backend/commands/foreigncmds.c:569-684](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/foreigncmds.c#L569-L684)

## Overview
Creates a new foreign-data wrapper (FDW) in the PostgreSQL system, inserting the FDW definition into the catalog and establishing all necessary dependencies and security constraints.

## Definition
```c
ObjectAddress CreateForeignDataWrapper(ParseState *pstate, CreateFdwStmt *stmt)
```

## Detailed Description
This function implements the CREATE FOREIGN DATA WRAPPER SQL command by creating a new FDW entry in the pg_foreign_data_wrapper system catalog. It performs comprehensive validation including superuser privilege checks, name uniqueness verification, and function option processing. The function handles the complete lifecycle of FDW creation including catalog insertion, dependency recording, and extension membership registration. It processes both handler and validator functions if specified, transforms generic options, and ensures proper ownership and security model compliance.

## Parameters / Member Variables
- `pstate`: ParseState context for parsing operations and error reporting
- `stmt`: CreateFdwStmt structure containing the parsed CREATE FOREIGN DATA WRAPPER statement details including name, options, and function specifications

## Dependencies
- Functions called/Symbols referenced:
  - [CreateFdwStmt](CreateFdwStmt.md)
  - [superuser](../s/superuser.md)
  - [GetForeignDataWrapperByName](../G/GetForeignDataWrapperByName.md)
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md)
  - [namein](../n/namein.md)
  - DirectFunctionCall1
  - [CStringGetDatum](CStringGetDatum.md)
  - [parse_func_options](../p/parse_func_options.md)
  - [transformGenericOptions](../t/transformGenericOptions.md)
  - PointerIsValid
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [CatalogTupleInsert](CatalogTupleInsert.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [recordDependencyOn](../r/recordDependencyOn.md)
  - [recordDependencyOnOwner](../r/recordDependencyOnOwner.md)
  - [recordDependencyOnCurrentExtension](../r/recordDependencyOnCurrentExtension.md)
  - InvokeObjectPostCreateHook
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
- Requires superuser privileges to execute; regular users cannot create FDWs
- Enforces FDW name uniqueness across the entire database cluster
- Automatically assigns the effective user ID as the owner (cannot be overridden during creation)
- Records dependencies on handler and validator functions if specified
- Supports extension membership through recordDependencyOnCurrentExtension
- Triggers object creation hooks for extensibility
- Returns an ObjectAddress for the newly created FDW for further reference
- Part of PostgreSQL's Foreign Data Wrapper infrastructure enabling integration with external data sources

## Simplified Source

```c
ObjectAddress
CreateForeignDataWrapper(ParseState *pstate, CreateFdwStmt *stmt)
{
    Relation rel;
    Datum values[Natts_pg_foreign_data_wrapper];
    bool nulls[Natts_pg_foreign_data_wrapper];
    HeapTuple tuple;
    Oid fdwId, ownerId;
    bool handler_given, validator_given;
    Oid fdwhandler, fdwvalidator;
    Datum fdwoptions;
    ObjectAddress myself, referenced;

    rel = table_open(ForeignDataWrapperRelationId, RowExclusiveLock);

    // Must be superuser
    if (!superuser()) {
        ereport(ERROR, "permission denied to create foreign-data wrapper");
    }

    ownerId = GetUserId();

    // Check for duplicate name
    if (GetForeignDataWrapperByName(stmt->fdwname, true) != NULL) {
        ereport(ERROR, "foreign-data wrapper already exists");
    }

    // Prepare tuple values
    memset(values, 0, sizeof(values));
    memset(nulls, false, sizeof(nulls));

    fdwId = GetNewOidWithIndex(rel, ForeignDataWrapperOidIndexId,
                               Anum_pg_foreign_data_wrapper_oid);
    values[Anum_pg_foreign_data_wrapper_oid - 1] = ObjectIdGetDatum(fdwId);
    values[Anum_pg_foreign_data_wrapper_fdwname - 1] =
        DirectFunctionCall1(namein, CStringGetDatum(stmt->fdwname));
    values[Anum_pg_foreign_data_wrapper_fdwowner - 1] = ObjectIdGetDatum(ownerId);

    // Parse and validate handler/validator functions
    parse_func_options(pstate, stmt->func_options,
                       &handler_given, &fdwhandler,
                       &validator_given, &fdwvalidator);

    values[Anum_pg_foreign_data_wrapper_fdwhandler - 1] = ObjectIdGetDatum(fdwhandler);
    values[Anum_pg_foreign_data_wrapper_fdwvalidator - 1] = ObjectIdGetDatum(fdwvalidator);

    nulls[Anum_pg_foreign_data_wrapper_fdwacl - 1] = true;

    // Transform and validate options
    fdwoptions = transformGenericOptions(ForeignDataWrapperRelationId,
                                        PointerGetDatum(NULL),
                                        stmt->options,
                                        fdwvalidator);

    if (PointerIsValid(DatumGetPointer(fdwoptions)))
        values[Anum_pg_foreign_data_wrapper_fdwoptions - 1] = fdwoptions;
    else
        nulls[Anum_pg_foreign_data_wrapper_fdwoptions - 1] = true;

    // Insert catalog entry
    tuple = heap_form_tuple(rel->rd_att, values, nulls);
    CatalogTupleInsert(rel, tuple);
    heap_freetuple(tuple);

    // Record dependencies
    myself.classId = ForeignDataWrapperRelationId;
    myself.objectId = fdwId;
    myself.objectSubId = 0;

    if (OidIsValid(fdwhandler)) {
        referenced.classId = ProcedureRelationId;
        referenced.objectId = fdwhandler;
        referenced.objectSubId = 0;
        recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
    }

    if (OidIsValid(fdwvalidator)) {
        referenced.classId = ProcedureRelationId;
        referenced.objectId = fdwvalidator;
        referenced.objectSubId = 0;
        recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
    }

    recordDependencyOnOwner(ForeignDataWrapperRelationId, fdwId, ownerId);
    recordDependencyOnCurrentExtension(&myself, false);

    // Invoke creation hook
    InvokeObjectPostCreateHook(ForeignDataWrapperRelationId, fdwId, 0);

    table_close(rel, RowExclusiveLock);
    return myself;
}
```