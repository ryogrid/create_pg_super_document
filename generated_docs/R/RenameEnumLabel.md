# RenameEnumLabel

## Location
[src/backend/catalog/pg_enum.c:607-689](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_enum.c#L607-L689)

## Overview
Renames a label in an enum type by updating the pg_enum catalog table while ensuring the new label name is valid and doesn't already exist.

## Definition

```c
void
RenameEnumLabel(Oid enumTypeOid,
				const char *oldVal,
				const char *newVal)
```
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
  - [LockDatabaseObject](../L/LockDatabaseObject.md): Acquires exclusive lock on the enum type
  - [table_open](../t/table_open.md): Opens the pg_enum relation for modification
  - SearchSysCacheList1: Retrieves all enum values for the given type
  - [heap_copytuple](../h/heap_copytuple.md): Creates a writable copy of the tuple
  - [namestrcpy](../n/namestrcpy.md): Copies the new name into the tuple structure
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md): Updates the tuple in the pg_enum catalog
  - [heap_freetuple](../h/heap_freetuple.md): Frees the temporary tuple memory
  - [ReleaseCatCacheList](ReleaseCatCacheList.md): Releases the system cache list
- Called from (representative examples):
  - [AlterEnum](../A/AlterEnum.md): Main entry point for enum alteration commands

## Notes and Other Information
- The function holds an exclusive lock on the enum type until transaction commit to ensure consistency
- Provides detailed error messages for invalid label names, non-existent old labels, and duplicate new labels
- The sort order of enum values is not affected by label renaming
- Changes are transactional and will be rolled back if the transaction fails
- Maximum label length is constrained by NAMEDATALEN-1 (typically 63 bytes)