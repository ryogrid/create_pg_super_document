# binary_upgrade_set_next_index_pg_class_oid

## Location
[src/backend/utils/adt/pg_upgrade_support.c:120-130](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_upgrade_support.c#L120-L130)

## Overview
Sets the next index relation OID to be used during binary upgrade operations for new indexes in the pg_class catalog.

## Definition
```c
Datum binary_upgrade_set_next_index_pg_class_oid(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is part of PostgreSQL's binary upgrade infrastructure, specifically designed to handle index OID preservation during cluster upgrades. It sets the global variable `binary_upgrade_next_index_pg_class_oid` to a specific OID value that will be used when creating the next index during the upgrade process.

Index OIDs must be preserved during binary upgrades to maintain referential integrity within system catalogs and to ensure that existing references to indexes (in statistics, constraints, etc.) remain valid after the upgrade. This function works in conjunction with the index creation routines to ensure deterministic OID assignment during the upgrade process.

The function can only be called when the server is running in binary upgrade mode and stores the provided OID for later consumption by index creation code.

## Parameters / Member Variables
- `reloid` (PG_GETARG_OID(0)): The OID value to be assigned to the next index relation created during binary upgrade

## Dependencies
- Functions called/Symbols referenced:
  - CHECK_IS_BINARY_UPGRADE (macro that verifies binary upgrade mode is active)
  - PG_GETARG_OID (extracts OID argument from function call)
  - PG_RETURN_VOID (returns void result)
- Global variable modified:
  - binary_upgrade_next_index_pg_class_oid (defined in src/backend/catalog/index.c:84)
- Called from:
  - pg_upgrade utility during cluster upgrade operations
  - Used in SQL scripts generated during binary upgrade process

## Notes and Other Information
- This function is only available when PostgreSQL is compiled with binary upgrade support
- The global variable `binary_upgrade_next_index_pg_class_oid` is declared in src/include/catalog/binary_upgrade.h:28
- The variable is consumed by index creation code in src/backend/catalog/index.c around lines 938-944
- There is a comment in heap.c:1203 referencing this mechanism for index OID assignment
- Error is raised if called outside binary upgrade mode
- After the OID is used for index creation, the global variable is reset to InvalidOid
- This mechanism ensures that indexes maintain their original OIDs from the source cluster in the upgraded cluster
- Works in conjunction with similar functions for heap tables and toast tables to provide comprehensive OID preservation

## Simplified Source

```c
Datum binary_upgrade_set_next_index_pg_class_oid(PG_FUNCTION_ARGS)
{
    Oid reloid = PG_GETARG_OID(0);

    // Verify we're in binary upgrade mode
    CHECK_IS_BINARY_UPGRADE;

    // Store the OID for the next index to be created
    binary_upgrade_next_index_pg_class_oid = reloid;

    PG_RETURN_VOID();
}
```