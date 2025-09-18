# IsThereCollationInNamespace

## Location
src/backend/commands/collationcmds.c: 400 - 427

## Overview
IsThereCollationInNamespace checks for name conflicts when renaming or moving collations, ensuring no duplicate collation names exist in the target namespace.

## Definition


## Detailed Description
This utility function performs duplicate name checking for collation operations like ALTER COLLATION RENAME and ALTER COLLATION SET SCHEMA. It searches the system catalog to verify that no collation with the same name already exists in the specified namespace. The function performs two separate checks:
1. Checks for collations with the same name and current database encoding
2. Checks for collations with the same name and any encoding (-1)

If either check finds a match, it raises an appropriate error with detailed information about the conflict.

## Parameters / Member Variables
- : The name of the collation to check for conflicts
- : The OID of the namespace where the collision check is performed

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCacheExists3
  - CStringGetDatum
  - GetDatabaseEncoding
  - GetDatabaseEncodingName
  - get_namespace_name
- Called from (representative examples):
  - AlterObjectRename_internal
  - AlterObjectNamespace_internal

## Notes and Other Information
- Used as a subroutine for ALTER COLLATION SET SCHEMA and RENAME operations
- Performs encoding-specific conflict detection to handle both specific and any-encoding collations
- Raises ERRCODE_DUPLICATE_OBJECT errors with descriptive messages when conflicts are found
- Critical for maintaining collation name uniqueness within namespaces