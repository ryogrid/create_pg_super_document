# binary_upgrade_set_next_heap_pg_class_oid

## Location
[src/backend/utils/adt/pg_upgrade_support.c:98-108](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_upgrade_support.c#L98-L108)

## Overview
Sets the next heap relation OID to be used during binary upgrade operations for new heap tables in the pg_class catalog.

## Definition

```c
enumber = PG_GETARG_OID(0);
```
## Detailed Description
This function is part of PostgreSQL's binary upgrade infrastructure, which allows upgrading PostgreSQL installations without needing to dump and reload data. It sets the global variable  to a specific OID value that will be used when creating the next heap table during the upgrade process. This ensures that object OIDs are preserved across upgrades, maintaining system catalog consistency.

The function can only be called when the server is running in binary upgrade mode ( is true). It takes a single OID parameter and stores it in the global variable for later use by heap table creation routines.

## Parameters / Member Variables
-  (PG_GETARG_OID(0)): The OID value to be assigned to the next heap relation created during binary upgrade

## Dependencies
- Functions called/Symbols referenced:
  - CHECK_IS_BINARY_UPGRADE (macro that verifies binary upgrade mode is active)
  - PG_GETARG_OID (extracts OID argument from function call)
  - PG_RETURN_VOID (returns void result)
- Global variable modified:
  - binary_upgrade_next_heap_pg_class_oid (defined in src/backend/catalog/heap.c:81)
- Called from:
  - pg_upgrade utility during cluster upgrade operations
  - Used in SQL scripts generated during binary upgrade process

## Notes and Other Information
- This function is only available when PostgreSQL is compiled with binary upgrade support
- The global variable is declared in src/include/catalog/binary_upgrade.h:26
- The variable is consumed by heap table creation code in src/backend/catalog/heap.c around lines 1227-1233
- Error is raised if called outside binary upgrade mode with message: "function can only be called when server is in binary upgrade mode"
- After the OID is used for table creation, the global variable is reset to InvalidOid

## Simplified Source

```c
Datum binary_upgrade_set_next_heap_pg_class_oid(PG_FUNCTION_ARGS) {
    // Extract the relation OID argument
    Oid reloid = PG_GETARG_OID(0);

    // Verify we're in binary upgrade mode (throws error if not)
    CHECK_IS_BINARY_UPGRADE;

    // Store the OID for the next heap relation creation
    binary_upgrade_next_heap_pg_class_oid = reloid;

    PG_RETURN_VOID();
}
```