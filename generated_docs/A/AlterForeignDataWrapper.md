# AlterForeignDataWrapper

## Location
[src/backend/commands/foreigncmds.c:685-848](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/foreigncmds.c#L685-L848)

## Overview
Modifies an existing foreign-data wrapper (FDW) by updating its handler function, validator function, and/or options in the PostgreSQL system catalog.

## Definition
```c
ObjectAddress AlterForeignDataWrapper(ParseState *pstate, AlterFdwStmt *stmt)
```

## Detailed Description
This function implements the ALTER FOREIGN DATA WRAPPER SQL command by modifying an existing FDW entry in the pg_foreign_data_wrapper system catalog. It performs comprehensive validation including superuser privilege checks, existence verification, and function option processing. The function handles selective updates to handler functions, validator functions, and generic options while maintaining dependency consistency. It provides warnings when changes might affect existing foreign tables or dependent objects, and properly manages function dependencies by removing old ones and creating new ones as needed.

## Parameters / Member Variables
- `pstate`: ParseState context for parsing operations and error reporting
- `stmt`: AlterFdwStmt structure containing the parsed ALTER FOREIGN DATA WRAPPER statement details including name, options, and function modifications

## Dependencies
- Functions called/Symbols referenced:
  - [AlterFdwStmt](AlterFdwStmt.md)
  - Form_pg_foreign_data_wrapper
  - [superuser](../s/superuser.md)
  - SearchSysCacheCopy1
  - [CStringGetDatum](../C/CStringGetDatum.md)
  - [parse_func_options](../p/parse_func_options.md)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - [transformGenericOptions](../t/transformGenericOptions.md)
  - PointerIsValid
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - ObjectAddressSet
  - [deleteDependencyRecordsForClass](../d/deleteDependencyRecordsForClass.md)
  - [recordDependencyOn](../r/recordDependencyOn.md)
  - InvokeObjectPostAlterHook
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
- Requires superuser privileges to execute; regular users cannot alter FDWs
- Validates FDW existence before attempting modifications
- Provides warnings when changing handler functions about potential behavior changes in existing foreign tables
- Provides warnings when changing validator functions about potential invalidation of dependent object options
- Supports partial updates - only specified attributes are modified
- Properly manages function dependencies by deleting old dependencies and creating new ones
- Uses heap_modify_tuple for selective column updates rather than full tuple replacement
- Triggers object alteration hooks for extensibility
- Returns ObjectAddress of the modified FDW for further reference
- Part of PostgreSQL's Foreign Data Wrapper infrastructure enabling dynamic reconfiguration of external data source integrations

## Simplified Source

```c
ObjectAddress AlterForeignDataWrapper(ParseState *pstate, AlterFdwStmt *stmt) {
    Relation rel;
    HeapTuple tp;
    Form_pg_foreign_data_wrapper fdwForm;
    Datum repl_val[Natts_pg_foreign_data_wrapper];
    bool repl_null[Natts_pg_foreign_data_wrapper];
    bool repl_repl[Natts_pg_foreign_data_wrapper];
    Oid fdwId;
    bool handler_given, validator_given;
    Oid fdwhandler, fdwvalidator;
    ObjectAddress myself;

    // Open catalog table with exclusive lock
    rel = table_open(ForeignDataWrapperRelationId, RowExclusiveLock);

    // Verify superuser permission
    if (!superuser())
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                       errmsg("permission denied to alter foreign-data wrapper \"%s\"",
                              stmt->fdwname)));

    // Find the FDW tuple
    tp = SearchSysCacheCopy1(FOREIGNDATAWRAPPERNAME, CStringGetDatum(stmt->fdwname));
    if (!HeapTupleIsValid(tp))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                       errmsg("foreign-data wrapper \"%s\" does not exist", stmt->fdwname)));

    fdwForm = (Form_pg_foreign_data_wrapper) GETSTRUCT(tp);
    fdwId = fdwForm->oid;

    // Initialize update arrays
    memset(repl_val, 0, sizeof(repl_val));
    memset(repl_null, false, sizeof(repl_null));
    memset(repl_repl, false, sizeof(repl_repl));

    // Parse function options (handler/validator)
    parse_func_options(pstate, stmt->func_options,
                       &handler_given, &fdwhandler,
                       &validator_given, &fdwvalidator);

    // Update handler function if specified
    if (handler_given) {
        repl_val[Anum_pg_foreign_data_wrapper_fdwhandler - 1] = ObjectIdGetDatum(fdwhandler);
        repl_repl[Anum_pg_foreign_data_wrapper_fdwhandler - 1] = true;
        ereport(WARNING, (errmsg("changing handler can change behavior of existing foreign tables")));
    }

    // Update validator function if specified
    if (validator_given) {
        repl_val[Anum_pg_foreign_data_wrapper_fdwvalidator - 1] = ObjectIdGetDatum(fdwvalidator);
        repl_repl[Anum_pg_foreign_data_wrapper_fdwvalidator - 1] = true;
        if (OidIsValid(fdwvalidator))
            ereport(WARNING, (errmsg("changing validator can invalidate dependent object options")));
    } else {
        fdwvalidator = fdwForm->fdwvalidator;
    }

    // Process generic options if specified
    if (stmt->options) {
        Datum datum = SysCacheGetAttr(FOREIGNDATAWRAPPEROID, tp,
                                     Anum_pg_foreign_data_wrapper_fdwoptions, &isnull);
        if (isnull)
            datum = PointerGetDatum(NULL);

        // Transform and validate options
        datum = transformGenericOptions(ForeignDataWrapperRelationId, datum,
                                       stmt->options, fdwvalidator);

        if (PointerIsValid(DatumGetPointer(datum)))
            repl_val[Anum_pg_foreign_data_wrapper_fdwoptions - 1] = datum;
        else
            repl_null[Anum_pg_foreign_data_wrapper_fdwoptions - 1] = true;

        repl_repl[Anum_pg_foreign_data_wrapper_fdwoptions - 1] = true;
    }

    // Update the catalog tuple
    tp = heap_modify_tuple(tp, RelationGetDescr(rel), repl_val, repl_null, repl_repl);
    CatalogTupleUpdate(rel, &tp->t_self, tp);
    heap_freetuple(tp);

    ObjectAddressSet(myself, ForeignDataWrapperRelationId, fdwId);

    // Update function dependencies if changed
    if (handler_given || validator_given) {
        ObjectAddress referenced;

        // Remove old function dependencies
        deleteDependencyRecordsForClass(ForeignDataWrapperRelationId, fdwId,
                                       ProcedureRelationId, DEPENDENCY_NORMAL);

        // Add new function dependencies
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
    }

    // Trigger post-alter hooks and cleanup
    InvokeObjectPostAlterHook(ForeignDataWrapperRelationId, fdwId, 0);
    table_close(rel, RowExclusiveLock);

    return myself;
}
```