# EnumValuesDelete

## Location
[src/backend/catalog/pg_enum.c:224-254](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_enum.c#L224-L254)

## Overview
Removes all pg_enum entries for a specified enum type during enum type deletion operations.

## Definition


## Detailed Description
EnumValuesDelete is a cleanup function that removes all enum value entries from the pg_enum catalog table for a given enum type. This function is typically called during DROP TYPE operations to ensure complete removal of enum-related catalog entries.

The function performs a systematic scan of the pg_enum table using the enum type OID as the search key, finding all enum values associated with the specified type and deleting them one by one. It uses the EnumTypIdLabelIndexId index for efficient lookup of all enum values belonging to the specified enum type.

The deletion process maintains catalog consistency by properly removing both the heap tuples and updating all associated indexes through the CatalogTupleDelete function.

## Parameters / Member Variables
- : The OID of the enum type whose values should be deleted from pg_enum

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md)
  - [systable_endscan](../s/systable_endscan.md)
  - table_close
- Called from:
  - [RemoveTypeById](../R/RemoveTypeById.md) (src/backend/commands/typecmds.c:676)

## Notes and Other Information
- The function uses RowExclusiveLock on the pg_enum table to ensure exclusive access during deletion
- Uses the EnumTypIdLabelIndexId index for efficient scanning of enum values by type OID
- Each enum value tuple is deleted individually using CatalogTupleDelete to maintain index consistency
- This function is part of the enum type cleanup process and should only be called when the enum type itself is being dropped
- The function does not perform any validation - it assumes the caller has verified that the enum type deletion is appropriate