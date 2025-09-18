# RemoveTypeById

## Location
[src/backend/commands/typecmds.c:657-696](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/typecmds.c#L657-L696)

## Overview
RemoveTypeById is the core function that handles the physical deletion of a type from the PostgreSQL system catalogs, including cleanup of specialized type data for enums and ranges.

## Definition


## Detailed Description
RemoveTypeById performs the actual removal of a type entry from the pg_type system catalog. This is a low-level function called by the dependency system after all dependency checks have been performed. The function handles the physical deletion of the type tuple and performs specialized cleanup for certain type categories:

- For enum types: Removes all associated pg_enum entries that define the enum values
- For range types: Removes the associated pg_range entry that defines the range properties

The function operates under a RowExclusiveLock on the pg_type relation to ensure safe concurrent access during type deletion. It uses the system cache for efficient type lookup and performs the actual catalog tuple deletion.

This function is typically called indirectly through the dependency management system when a DROP TYPE command is executed, rather than being called directly by user-facing commands.

## Parameters / Member Variables
- : The OID of the type to be removed from the system catalogs

## Dependencies
- Functions called/Symbols referenced:
  - table_open: Opens the pg_type system catalog with appropriate locking
  - [SearchSysCache1](../S/SearchSysCache1.md): Locates the type tuple in the system cache
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md): Removes the type tuple from the catalog
  - [EnumValuesDelete](../E/EnumValuesDelete.md): Removes pg_enum entries for enum types
  - [RangeDelete](RangeDelete.md): Removes pg_range entries for range types
  - [ReleaseSysCache](ReleaseSysCache.md): Releases the cached tuple
  - table_close: Closes the catalog relation

- Called from (representative examples):
  - [doDeletion](../d/doDeletion.md): Main dependency system deletion handler

## Notes and Other Information
- This is a low-level catalog manipulation function that should not be called directly
- The function assumes all dependency and permission checks have been performed
- Enum and range type cleanup is handled manually since these don't use standard dependency tracking
- Uses system cache for performance during type lookup
- Operates under exclusive row locking to prevent concurrent modifications
- Part of the broader object deletion infrastructure in PostgreSQL
- The actual DROP TYPE command goes through higher-level functions that handle dependencies first