# extension_config_remove

## Location
[src/backend/commands/extension.c:2607-2771](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/extension.c#L2607-L2771)

## Overview
Removes a specified table OID from an extension's extconfig array, effectively unregistering the table as a configuration table that should be dumped during pg_dump operations.

## Definition

```c
struct_array_builtin(a, OIDOID, &dvalues, NULL, &nelems);
```
## Detailed Description
This internal static function removes a table from an extension's configuration table list by modifying the extconfig and extcondition arrays in the pg_extension catalog. When a table is removed from extconfig, it will no longer be included in pg_dump output as configuration data, meaning only the table structure (if it remains part of the extension) will be recreated, not its data.

The function searches for the specified table OID in the extconfig array and removes both the table OID and its corresponding WHERE condition from the extcondition array. It maintains array consistency by compacting the arrays after removal. If the table is not found in extconfig, the function returns without making changes.

## Parameters / Member Variables
-  (Oid): OID of the extension from which to remove the configuration table
-  (Oid): OID of the table to remove from the extension's configuration list

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md): Opens pg_extension catalog for modification
  - [ScanKeyInit](../S/ScanKeyInit.md): Initializes scan key for extension lookup
  - [systable_beginscan](../s/systable_beginscan.md)/systable_getnext: Scans for extension tuple
  - [heap_getattr](../h/heap_getattr.md): Retrieves extconfig and extcondition arrays
  - DatumGetArrayTypeP: Converts datum to array type
  - [deconstruct_array_builtin](../d/deconstruct_array_builtin.md): Breaks down arrays into individual elements
  - [construct_array_builtin](../c/construct_array_builtin.md): Rebuilds arrays after element removal
  - [heap_modify_tuple](../h/heap_modify_tuple.md): Updates extension tuple
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md): Commits changes to catalog
- Called from (representative examples):
  - [ExecAlterExtensionContentsRecurse](../E/ExecAlterExtensionContentsRecurse.md): Used during ALTER EXTENSION DROP operations

## Notes and Other Information
- This is a static internal function, not exposed as a SQL-callable function
- Currently invoked only from ALTER EXTENSION DROP operations
- Maintains synchronization between extconfig and extcondition arrays
- Uses RowExclusiveLock to ensure safe concurrent access to pg_extension catalog
- Handles edge cases like removing the last configuration table (sets arrays to NULL)
- Validates array structure and dimensions before modification
- The function comment suggests it could be exposed as a public function in the future
- Located in src/backend/commands/extension.c:2607-2771

## Simplified Source

```c
static void extension_config_remove(Oid extensionoid, Oid tableoid) {
    Relation extRel;
    ScanKeyData key[1];
    SysScanDesc extScan;
    HeapTuple extTup;
    ArrayType *a;
    int arrayLength, arrayIndex = -1;

    // Open pg_extension catalog and find the extension
    extRel = table_open(ExtensionRelationId, RowExclusiveLock);

    ScanKeyInit(&key[0], Anum_pg_extension_oid, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(extensionoid));

    extScan = systable_beginscan(extRel, ExtensionOidIndexId, true, NULL, 1, key);
    extTup = systable_getnext(extScan);

    if (!HeapTupleIsValid(extTup))
        elog(ERROR, "could not find tuple for extension %u", extensionoid);

    // Get extconfig array and search for table OID
    Datum arrayDatum = heap_getattr(extTup, Anum_pg_extension_extconfig,
                                   RelationGetDescr(extRel), &isnull);

    if (!isnull) {
        a = DatumGetArrayTypeP(arrayDatum);
        arrayLength = ARR_DIMS(a)[0];

        // Validate array structure
        if (ARR_NDIM(a) != 1 || ARR_LBOUND(a)[0] != 1 || arrayLength < 0 ||
            ARR_HASNULL(a) || ARR_ELEMTYPE(a) != OIDOID)
            elog(ERROR, "extconfig is not a 1-D Oid array");

        // Find table OID in array
        Oid *arrayData = (Oid *) ARR_DATA_PTR(a);
        for (int i = 0; i < arrayLength; i++) {
            if (arrayData[i] == tableoid) {
                arrayIndex = i;
                break;
            }
        }
    }

    // If table not found, nothing to do
    if (arrayIndex < 0) {
        systable_endscan(extScan);
        table_close(extRel, RowExclusiveLock);
        return;
    }

    // Prepare tuple update arrays
    Datum repl_val[Natts_pg_extension] = {0};
    bool repl_null[Natts_pg_extension] = {false};
    bool repl_repl[Natts_pg_extension] = {false};

    // Handle extconfig array modification
    if (arrayLength <= 1) {
        // Removing last element - set to NULL
        repl_null[Anum_pg_extension_extconfig - 1] = true;
    } else {
        // Remove element by compacting array
        Datum *dvalues;
        int nelems;
        deconstruct_array_builtin(a, OIDOID, &dvalues, NULL, &nelems);

        // Shift elements to remove target
        for (int i = arrayIndex; i < arrayLength - 1; i++)
            dvalues[i] = dvalues[i + 1];

        a = construct_array_builtin(dvalues, arrayLength - 1, OIDOID);
        repl_val[Anum_pg_extension_extconfig - 1] = PointerGetDatum(a);
    }
    repl_repl[Anum_pg_extension_extconfig - 1] = true;

    // Handle corresponding extcondition array modification
    arrayDatum = heap_getattr(extTup, Anum_pg_extension_extcondition,
                             RelationGetDescr(extRel), &isnull);

    if (arrayLength <= 1) {
        // Remove last element - set to NULL
        repl_null[Anum_pg_extension_extcondition - 1] = true;
    } else {
        // Remove corresponding condition element
        a = DatumGetArrayTypeP(arrayDatum);
        Datum *dvalues;
        int nelems;
        deconstruct_array_builtin(a, TEXTOID, &dvalues, NULL, &nelems);

        // Shift elements to remove target condition
        for (int i = arrayIndex; i < arrayLength - 1; i++)
            dvalues[i] = dvalues[i + 1];

        a = construct_array_builtin(dvalues, arrayLength - 1, TEXTOID);
        repl_val[Anum_pg_extension_extcondition - 1] = PointerGetDatum(a);
    }
    repl_repl[Anum_pg_extension_extcondition - 1] = true;

    // Update the extension tuple
    extTup = heap_modify_tuple(extTup, RelationGetDescr(extRel),
                              repl_val, repl_null, repl_repl);
    CatalogTupleUpdate(extRel, &extTup->t_self, extTup);

    systable_endscan(extScan);
    table_close(extRel, RowExclusiveLock);
}
```