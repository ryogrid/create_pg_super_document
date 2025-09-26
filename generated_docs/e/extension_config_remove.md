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