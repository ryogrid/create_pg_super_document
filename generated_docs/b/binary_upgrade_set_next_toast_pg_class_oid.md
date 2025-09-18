# binary_upgrade_set_next_toast_pg_class_oid

## Location
[src/backend/utils/adt/pg_upgrade_support.c:142-152](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_upgrade_support.c#L142-L152)

## Overview
Sets the next TOAST table OID to be used during binary upgrade operations for new TOAST tables in the pg_class catalog.

## Definition
```c
Datum binary_upgrade_set_next_toast_pg_class_oid(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is part of PostgreSQL's binary upgrade infrastructure, specifically designed to handle TOAST (The Oversized-Attribute Storage Technique) table OID preservation during cluster upgrades. TOAST tables are automatically created to store large values that exceed the page size limit for regular table storage.

The function sets the global variable `binary_upgrade_next_toast_pg_class_oid` to a specific OID value that will be used when creating the next TOAST table during the upgrade process. TOAST table OIDs must be preserved during binary upgrades to maintain the relationship between base tables and their associated TOAST tables, ensuring that large data values remain accessible after the upgrade.

The function can only be called when the server is running in binary upgrade mode and stores the provided OID for later consumption by TOAST table creation routines.

## Parameters / Member Variables
- `reloid` (PG_GETARG_OID(0)): The OID value to be assigned to the next TOAST table relation created during binary upgrade

## Dependencies
- Functions called/Symbols referenced:
  - CHECK_IS_BINARY_UPGRADE (macro that verifies binary upgrade mode is active)
  - PG_GETARG_OID (extracts OID argument from function call)
  - PG_RETURN_VOID (returns void result)
- Global variable modified:
  - binary_upgrade_next_toast_pg_class_oid (defined in src/backend/catalog/heap.c:82)
- Called from:
  - pg_upgrade utility during cluster upgrade operations
  - Used in SQL scripts generated during binary upgrade process

## Notes and Other Information
- This function is only available when PostgreSQL is compiled with binary upgrade support
- The global variable `binary_upgrade_next_toast_pg_class_oid` is declared in src/include/catalog/binary_upgrade.h:30
- The variable is consumed by heap table creation code in src/backend/catalog/heap.c around lines 1211-1214
- Also referenced in TOAST table creation logic in src/backend/catalog/toasting.c around line 184
- Error is raised if called outside binary upgrade mode
- After the OID is used for TOAST table creation, the global variable is reset to InvalidOid
- TOAST tables are essential for storing values larger than approximately 2KB in PostgreSQL
- This mechanism ensures that the relationship between base tables and their TOAST tables is preserved across upgrades
- Works as part of the comprehensive OID preservation system alongside heap table and index OID preservation functions