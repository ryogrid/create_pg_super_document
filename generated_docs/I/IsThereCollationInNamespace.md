# IsThereCollationInNamespace

## Location
[src/backend/commands/collationcmds.c:400-427](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/collationcmds.c#L400-L427)

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
  - [CStringGetDatum](../C/CStringGetDatum.md)
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md)
  - [GetDatabaseEncodingName](../G/GetDatabaseEncodingName.md)
  - [get_namespace_name](../g/get_namespace_name.md)
- Called from (representative examples):
  - [AlterObjectRename_internal](../A/AlterObjectRename_internal.md)
  - [AlterObjectNamespace_internal](../A/AlterObjectNamespace_internal.md)

## Notes and Other Information
- Used as a subroutine for ALTER COLLATION SET SCHEMA and RENAME operations
- Performs encoding-specific conflict detection to handle both specific and any-encoding collations
- Raises ERRCODE_DUPLICATE_OBJECT errors with descriptive messages when conflicts are found
- Critical for maintaining collation name uniqueness within namespaces