# binary_upgrade_set_next_heap_relfilenode

## Location
[src/backend/utils/adt/pg_upgrade_support.c:109-119](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_upgrade_support.c#L109-L119)

## Overview
Sets the next heap relation file node number to be used during binary upgrade operations for new heap tables.

## Definition
```c
Datum binary_upgrade_set_next_heap_relfilenode(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is part of PostgreSQL's binary upgrade infrastructure, responsible for preserving relfilenode numbers (physical file identifiers) across upgrades. It sets the global variable `binary_upgrade_next_heap_pg_class_relfilenumber` to a specific RelFileNumber value that will be used when creating the next heap table during the upgrade process.

Relfilenodes are critical for maintaining the physical storage mapping of database objects. During binary upgrades, it's essential to preserve these identifiers to ensure that the upgraded cluster can correctly access existing data files without requiring data migration.

The function can only be called when the server is running in binary upgrade mode and takes a RelFileNumber parameter, storing it for later consumption by heap table creation routines.

## Parameters / Member Variables
- `relfilenumber` (PG_GETARG_OID(0)): The RelFileNumber value to be assigned to the next heap relation created during binary upgrade

## Dependencies
- Functions called/Symbols referenced:
  - CHECK_IS_BINARY_UPGRADE (macro that verifies binary upgrade mode is active)
  - PG_GETARG_OID (extracts RelFileNumber argument from function call)
  - PG_RETURN_VOID (returns void result)
  - [RelFileNumber](../R/RelFileNumber.md) (type definition for relation file numbers)
- Global variable modified:
  - binary_upgrade_next_heap_pg_class_relfilenumber (defined in src/backend/catalog/heap.c:83)
- Called from:
  - pg_upgrade utility during cluster upgrade operations
  - Used in SQL scripts generated during binary upgrade process

## Notes and Other Information
- This function is only available when PostgreSQL is compiled with binary upgrade support
- The global variable `binary_upgrade_next_heap_pg_class_relfilenumber` is declared in src/include/catalog/binary_upgrade.h:27
- The variable is consumed by heap table creation code in src/backend/catalog/heap.c around lines 1237-1243
- Also used in relation cache management in src/backend/utils/cache/relcache.c around lines 3798-3804
- Error is raised if called outside binary upgrade mode
- After the RelFileNumber is used for table creation, the global variable is reset to InvalidRelFileNumber
- [RelFileNumber](../R/RelFileNumber.md) is distinct from OID - it identifies the physical storage files while OID identifies the logical database object

## Simplified Source

```c
Datum binary_upgrade_set_next_heap_relfilenode(PG_FUNCTION_ARGS) {
    // Extract the relfilenode number argument
    RelFileNumber relfilenumber = PG_GETARG_OID(0);

    // Verify we're in binary upgrade mode (throws error if not)
    CHECK_IS_BINARY_UPGRADE;

    // Store the relfilenode for the next heap relation creation
    binary_upgrade_next_heap_pg_class_relfilenumber = relfilenumber;

    PG_RETURN_VOID();
}
```