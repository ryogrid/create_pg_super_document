# AlterTableSpaceOptions

## Location
[src/backend/commands/tablespace.c:1015-1090](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablespace.c#L1015-L1090)

## Overview
Modifies the configuration options of an existing tablespace by updating the spcoptions field in the pg_tablespace system catalog.

## Definition
Oid AlterTableSpaceOptions(AlterTableSpaceOptionsStmt *stmt)

## Detailed Description
This function implements the ALTER TABLESPACE ... SET/RESET option functionality in PostgreSQL. It validates the existence of the specified tablespace, checks ownership permissions, and then processes the option changes. The function retrieves the current tablespace options, transforms them according to the ALTER statement (either setting new values or resetting to defaults), validates the new options using tablespace_reloptions(), and updates the system catalog. The operation is performed with row-exclusive locking to ensure consistency. After successful modification, it triggers post-alter hooks and performs proper resource cleanup.

## Parameters / Member Variables
- stmt: Pointer to AlterTableSpaceOptionsStmt containing the tablespace name, options to modify, and operation type (SET/RESET)

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md): Opens pg_tablespace relation with RowExclusiveLock
  - [ScanKeyInit](../S/ScanKeyInit.md): Initializes scan key for catalog lookup
  - [table_beginscan_catalog](../t/table_beginscan_catalog.md): Begins catalog table scan
  - [heap_getnext](../h/heap_getnext.md): Retrieves next heap tuple from scan
  - [object_ownercheck](../o/object_ownercheck.md): Verifies ownership permissions
  - [aclcheck_error](../a/aclcheck_error.md): Reports access control violations
  - [heap_getattr](../h/heap_getattr.md): Extracts attribute value from heap tuple
  - [transformRelOptions](../t/transformRelOptions.md): Processes relation option changes
  - [tablespace_reloptions](../t/tablespace_reloptions.md): Validates tablespace-specific options
  - [heap_modify_tuple](../h/heap_modify_tuple.md): Creates modified version of heap tuple
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md): Updates tuple in system catalog
  - InvokeObjectPostAlterHook: Triggers post-alter event hooks
  - [heap_freetuple](../h/heap_freetuple.md): Frees allocated heap tuple memory
  - [table_endscan](../t/table_endscan.md): Ends table scan
  - [table_close](../t/table_close.md): Closes relation

- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md): Main utility command processing handler

## Notes and Other Information
- Requires ownership of the tablespace to perform option modifications
- Validates new options through tablespace_reloptions() before applying changes
- Supports both SET and RESET operations through the isReset flag in the statement
- Uses heap_modify_tuple() to construct the updated catalog tuple efficiently
- Properly handles NULL values when options are reset to defaults
- Integrates with PostgreSQL's object dependency system through post-alter hooks
- Returns the OID of the modified tablespace for further processing
- Part of PostgreSQL's DDL infrastructure for tablespace configuration management
- Maintains catalog consistency through appropriate locking and transaction handling

## Simplified Source

```c
Oid AlterTableSpaceOptions(AlterTableSpaceOptionsStmt *stmt) {
    Relation rel;
    HeapTuple tup, newtuple;
    Oid tablespaceoid;
    Datum newOptions;

    // Open pg_tablespace catalog with exclusive lock
    rel = table_open(TableSpaceRelationId, RowExclusiveLock);

    // Find the tablespace by name
    ScanKeyData entry[1];
    ScanKeyInit(&entry[0], Anum_pg_tablespace_spcname,
                BTEqualStrategyNumber, F_NAMEEQ,
                CStringGetDatum(stmt->tablespacename));

    TableScanDesc scandesc = table_beginscan_catalog(rel, 1, entry);
    tup = heap_getnext(scandesc, ForwardScanDirection);

    if (!HeapTupleIsValid(tup))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                       errmsg("tablespace \"%s\" does not exist",
                              stmt->tablespacename)));

    tablespaceoid = ((Form_pg_tablespace) GETSTRUCT(tup))->oid;

    // Check ownership permissions
    if (!object_ownercheck(TableSpaceRelationId, tablespaceoid, GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLESPACE,
                      stmt->tablespacename);

    // Transform the options (SET or RESET)
    Datum currentOptions = heap_getattr(tup, Anum_pg_tablespace_spcoptions,
                                       RelationGetDescr(rel), &isnull);
    newOptions = transformRelOptions(isnull ? (Datum) 0 : currentOptions,
                                   stmt->options, NULL, NULL, false,
                                   stmt->isReset);

    // Validate the new options
    tablespace_reloptions(newOptions, true);

    // Build and update the catalog tuple
    Datum repl_val[Natts_pg_tablespace];
    bool repl_null[Natts_pg_tablespace];
    bool repl_repl[Natts_pg_tablespace];

    memset(repl_null, false, sizeof(repl_null));
    memset(repl_repl, false, sizeof(repl_repl));

    if (newOptions != (Datum) 0)
        repl_val[Anum_pg_tablespace_spcoptions - 1] = newOptions;
    else
        repl_null[Anum_pg_tablespace_spcoptions - 1] = true;
    repl_repl[Anum_pg_tablespace_spcoptions - 1] = true;

    newtuple = heap_modify_tuple(tup, RelationGetDescr(rel),
                                repl_val, repl_null, repl_repl);

    // Update the catalog and trigger hooks
    CatalogTupleUpdate(rel, &newtuple->t_self, newtuple);
    InvokeObjectPostAlterHook(TableSpaceRelationId, tablespaceoid, 0);

    // Cleanup
    heap_freetuple(newtuple);
    table_endscan(scandesc);
    table_close(rel, NoLock);

    return tablespaceoid;
}
```