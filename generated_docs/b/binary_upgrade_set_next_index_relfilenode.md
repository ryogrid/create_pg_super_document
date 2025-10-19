# binary_upgrade_set_next_index_relfilenode

## Location
[src/backend/utils/adt/pg_upgrade_support.c:131-141](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_upgrade_support.c#L131-L141)

## Overview
Sets the next index relation file node number to be used during binary upgrade operations for new indexes.

## Definition
```c
Datum binary_upgrade_set_next_index_relfilenode(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is part of PostgreSQL's binary upgrade infrastructure, responsible for preserving index relfilenode numbers (physical file identifiers) across upgrades. It sets the global variable `binary_upgrade_next_index_pg_class_relfilenumber` to a specific RelFileNumber value that will be used when creating the next index during the upgrade process.

Index relfilenodes are critical for maintaining the physical storage mapping of index files. During binary upgrades, preserving these identifiers ensures that the upgraded cluster can correctly access existing index data files without requiring index rebuilding or data migration. This is particularly important for large databases where rebuilding indexes would be time-consuming.

The function can only be called when the server is running in binary upgrade mode and takes a RelFileNumber parameter, storing it for later consumption by index creation routines.

## Parameters / Member Variables
- `relfilenumber` (PG_GETARG_OID(0)): The RelFileNumber value to be assigned to the next index relation created during binary upgrade

## Dependencies
- Functions called/Symbols referenced:
  - CHECK_IS_BINARY_UPGRADE (macro that verifies binary upgrade mode is active)
  - PG_GETARG_OID (extracts RelFileNumber argument from function call)
  - PG_RETURN_VOID (returns void result)
  - [RelFileNumber](../R/RelFileNumber.md) (type definition for relation file numbers)
- Global variable modified:
  - binary_upgrade_next_index_pg_class_relfilenumber (defined in src/backend/catalog/index.c:85)
- Called from:
  - pg_upgrade utility during cluster upgrade operations
  - Used in SQL scripts generated during binary upgrade process

## Notes and Other Information
- This function is only available when PostgreSQL is compiled with binary upgrade support
- The global variable `binary_upgrade_next_index_pg_class_relfilenumber` is declared in src/include/catalog/binary_upgrade.h:29
- The variable is consumed by index creation code in src/backend/catalog/index.c around lines 948-953
- Also used in relation cache management in src/backend/utils/cache/relcache.c around lines 3788-3794
- Error is raised if called outside binary upgrade mode
- After the RelFileNumber is used for index creation, the global variable is reset to InvalidRelFileNumber
- [RelFileNumber](../R/RelFileNumber.md) preservation is essential for avoiding index rebuilds during upgrades, which can be a major performance bottleneck
- Works in conjunction with heap and toast relfilenode preservation functions to provide comprehensive physical file mapping preservation
- Note: There appears to be a type inconsistency in relcache.c:3794 where InvalidOid is used instead of InvalidRelFileNumber

## Simplified Source

```c
Datum binary_upgrade_set_next_index_relfilenode(PG_FUNCTION_ARGS)
{
    RelFileNumber relfilenumber = PG_GETARG_OID(0);

    // Verify we're in binary upgrade mode
    CHECK_IS_BINARY_UPGRADE;

    // Store the relfilenode for the next index to be created
    binary_upgrade_next_index_pg_class_relfilenumber = relfilenumber;

    PG_RETURN_VOID();
}
```