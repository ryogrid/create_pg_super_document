# RenameEnumLabel

## Location
src/backend/catalog/pg_enum.c: 607 - 689

## Overview
Renames a label in an enum type by updating the pg_enum catalog table while ensuring the new label name is valid and doesn't already exist.

## Definition


## Detailed Description
This function handles the renaming of an existing enum label within a PostgreSQL enum type. It performs comprehensive validation to ensure the operation is safe and consistent:

1. **Length validation**: Checks that the new label doesn't exceed NAMEDATALEN-1 bytes
2. **Concurrency control**: Acquires an exclusive lock on the enum type to prevent concurrent modifications
3. **Existence verification**: Validates that the old label exists and the new label doesn't already exist
4. **Catalog update**: Updates the pg_enum system catalog with the new label name

The function uses the system cache to efficiently retrieve all existing enum values and performs in-memory validation before making any permanent changes to the catalog.

## Parameters / Member Variables
- : Object identifier of the enum type containing the label to be renamed
- : Current name of the enum label that should be renamed
- : New name for the enum label (must be unique within the enum type)

## Dependencies
- Functions called/Symbols referenced:
  - LockDatabaseObject: Acquires exclusive lock on the enum type
  - table_open: Opens the pg_enum relation for modification
  - SearchSysCacheList1: Retrieves all enum values for the given type
  - heap_copytuple: Creates a writable copy of the tuple
  - namestrcpy: Copies the new name into the tuple structure
  - CatalogTupleUpdate: Updates the tuple in the pg_enum catalog
  - heap_freetuple: Frees the temporary tuple memory
  - ReleaseCatCacheList: Releases the system cache list
- Called from (representative examples):
  - AlterEnum: Main entry point for enum alteration commands

## Notes and Other Information
- The function holds an exclusive lock on the enum type until transaction commit to ensure consistency
- Provides detailed error messages for invalid label names, non-existent old labels, and duplicate new labels
- The sort order of enum values is not affected by label renaming
- Changes are transactional and will be rolled back if the transaction fails
- Maximum label length is constrained by NAMEDATALEN-1 (typically 63 bytes)