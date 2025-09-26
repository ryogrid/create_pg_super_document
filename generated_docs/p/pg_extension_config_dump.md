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
  - get_rel_name: Gets table name from OID
  - getExtensionOfObject: Verifies table belongs to current extension
  - table_open: Opens pg_extension catalog for modification
  - systable_beginscan/systable_getnext: Scans for extension tuple
  - heap_getattr: Retrieves extconfig and extcondition arrays
  - construct_array_builtin: Creates new arrays when needed
  - array_set: Modifies existing arrays
  - heap_modify_tuple: Updates extension tuple
  - CatalogTupleUpdate: Commits changes to catalog
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