# pg_extension_config_dump

## Location
[src/backend/commands/extension.c:2424-2606](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/extension.c#L2424-L2606)

## Overview
Records information about a configuration table that belongs to an extension being created, specifying that its contents should be dumped in whole or in part during pg_dump operations.

## Definition

```c
struct_array_builtin(&elementDatum, 1, OIDOID);
```
## Detailed Description
This function is a PostgreSQL SQL-callable function that can only be invoked from within an extension's SQL script during CREATE EXTENSION execution. It registers a table as a configuration table for the extension, meaning that the table's data (subject to an optional WHERE condition) will be included in pg_dump output even though the table structure itself is part of the extension.

The function modifies the pg_extension catalog entry by updating the extconfig and extcondition arrays. The extconfig array stores the OIDs of configuration tables, while extcondition stores corresponding WHERE conditions that filter which rows should be dumped. If a table is already registered, the function updates its WHERE condition.

This mechanism is essential for extensions that create tables whose structure is managed by the extension but whose data represents user configuration that should be preserved across dump/restore operations.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  -  (Oid): OID of the table to register as a configuration table
  -  (text): WHERE condition to filter rows for dumping (can be empty for all rows)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID: Extracts table OID from function arguments
  - PG_GETARG_TEXT_PP: Extracts WHERE condition text from function arguments
  - [get_rel_name](../g/get_rel_name.md): Gets table name from OID
  - [getExtensionOfObject](../g/getExtensionOfObject.md): Verifies table belongs to current extension
  - [table_open](../t/table_open.md): Opens pg_extension catalog for modification
  - [systable_beginscan](../s/systable_beginscan.md)/systable_getnext: Scans for extension tuple
  - [heap_getattr](../h/heap_getattr.md): Retrieves extconfig and extcondition arrays
  - [construct_array_builtin](../c/construct_array_builtin.md): Creates new arrays when needed
  - [array_set](../a/array_set.md): Modifies existing arrays
  - [heap_modify_tuple](../h/heap_modify_tuple.md): Updates extension tuple
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md): Commits changes to catalog
- Called from:
  - Extension SQL scripts via pg_extension_config_dump() function calls

## Notes and Other Information
- Can only be called during CREATE EXTENSION execution (enforced by creating_extension flag)
- Verifies that the specified table belongs to the extension being created
- Maintains synchronization between extconfig and extcondition arrays
- Supports both adding new configuration tables and updating existing ones
- Uses RowExclusiveLock to ensure safe concurrent access to pg_extension catalog
- The WHERE condition is stored as text and evaluated during pg_dump operations
- Located in src/backend/commands/extension.c:2424-2606

## Simplified Source

```c
Datum
pg_extension_config_dump(PG_FUNCTION_ARGS)
{
    Oid tableoid = PG_GETARG_OID(0);
    text *wherecond = PG_GETARG_TEXT_PP(1);
    char *tablename;
    Relation extRel;
    ScanKeyData key[1];
    SysScanDesc extScan;
    HeapTuple extTup;
    Datum arrayDatum, elementDatum;
    int arrayLength, arrayIndex;
    bool isnull;
    Datum repl_val[Natts_pg_extension];
    bool repl_null[Natts_pg_extension];
    bool repl_repl[Natts_pg_extension];
    ArrayType *a;

    // Validate function called during extension creation
    if (!creating_extension)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("pg_extension_config_dump() can only be called from an SQL script executed by CREATE EXTENSION")));

    // Verify table exists and belongs to current extension
    tablename = get_rel_name(tableoid);
    if (tablename == NULL)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE),
                errmsg("OID %u does not refer to a table", tableoid)));

    if (getExtensionOfObject(RelationRelationId, tableoid) != CurrentExtensionObject)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("table \"%s\" is not a member of the extension being created", tablename)));

    // Find and update the pg_extension tuple
    extRel = table_open(ExtensionRelationId, RowExclusiveLock);
    ScanKeyInit(&key[0], Anum_pg_extension_oid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(CurrentExtensionObject));
    extScan = systable_beginscan(extRel, ExtensionOidIndexId, true, NULL, 1, key);
    extTup = systable_getnext(extScan);

    if (!HeapTupleIsValid(extTup))
        elog(ERROR, "could not find tuple for extension %u", CurrentExtensionObject);

    memset(repl_val, 0, sizeof(repl_val));
    memset(repl_null, false, sizeof(repl_null));
    memset(repl_repl, false, sizeof(repl_repl));

    // Update extconfig array (add or replace table OID)
    elementDatum = ObjectIdGetDatum(tableoid);
    arrayDatum = heap_getattr(extTup, Anum_pg_extension_extconfig, RelationGetDescr(extRel), &isnull);

    if (isnull)
    {
        // Create new extconfig array
        arrayLength = 0;
        arrayIndex = 1;
        a = construct_array_builtin(&elementDatum, 1, OIDOID);
    }
    else
    {
        // Modify existing extconfig array
        Oid *arrayData;
        int i;

        a = DatumGetArrayTypeP(arrayDatum);
        arrayLength = ARR_DIMS(a)[0];
        arrayData = (Oid *) ARR_DATA_PTR(a);

        // Find existing entry or append new one
        arrayIndex = arrayLength + 1;
        for (i = 0; i < arrayLength; i++)
        {
            if (arrayData[i] == tableoid)
            {
                arrayIndex = i + 1;
                break;
            }
        }

        a = array_set(a, 1, &arrayIndex, elementDatum, false, -1, sizeof(Oid), true, TYPALIGN_INT);
    }
    repl_val[Anum_pg_extension_extconfig - 1] = PointerGetDatum(a);
    repl_repl[Anum_pg_extension_extconfig - 1] = true;

    // Update extcondition array (matching WHERE condition)
    elementDatum = PointerGetDatum(wherecond);
    arrayDatum = heap_getattr(extTup, Anum_pg_extension_extcondition, RelationGetDescr(extRel), &isnull);

    if (isnull)
    {
        a = construct_array_builtin(&elementDatum, 1, TEXTOID);
    }
    else
    {
        a = DatumGetArrayTypeP(arrayDatum);
        a = array_set(a, 1, &arrayIndex, elementDatum, false, -1, -1, false, TYPALIGN_INT);
    }
    repl_val[Anum_pg_extension_extcondition - 1] = PointerGetDatum(a);
    repl_repl[Anum_pg_extension_extcondition - 1] = true;

    // Update the extension tuple
    extTup = heap_modify_tuple(extTup, RelationGetDescr(extRel), repl_val, repl_null, repl_repl);
    CatalogTupleUpdate(extRel, &extTup->t_self, extTup);

    systable_endscan(extScan);
    table_close(extRel, RowExclusiveLock);

    PG_RETURN_VOID();
}
```