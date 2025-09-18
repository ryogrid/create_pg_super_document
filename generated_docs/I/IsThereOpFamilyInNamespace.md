# IsThereOpFamilyInNamespace

## Location
src/backend/commands/opclasscmds.c: 1828 - 1842

## Overview
Validates that an operator family with the specified name and access method does not already exist in a given namespace, raising an error if a duplicate is found.

## Definition


## Detailed Description
This function serves as a validation subroutine used during ALTER OPERATOR FAMILY operations, specifically for SET SCHEMA and RENAME operations. It performs a uniqueness check by searching the system catalogs to determine if an operator family with the given name and access method already exists in the target namespace.

The function uses the system cache (OPFAMILYAMNAMENSP) to efficiently lookup existing operator families. If a duplicate is found, it immediately raises an ERROR with code ERRCODE_DUPLICATE_OBJECT, providing a detailed error message that includes the operator family name, access method name, and schema name.

This validation prevents naming conflicts and maintains the integrity of the operator family namespace organization within PostgreSQL's type system.

## Parameters / Member Variables
- : The name of the operator family to check for existence
- : The OID of the access method associated with the operator family
- : The OID of the namespace (schema) where the existence check is performed

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCacheExists3 (system cache lookup function)
  - CStringGetDatum (datum conversion utility)
  - ObjectIdGetDatum (datum conversion utility)
  - get_am_name (retrieves access method name for error reporting)
  - get_namespace_name (retrieves schema name for error reporting)
  - ereport (error reporting function)
- Called from (representative examples):
  - AlterObjectRename_internal (when renaming operator families)
  - AlterObjectNamespace_internal (when moving operator families to different schemas)

## Notes and Other Information
- This function is specifically designed for ALTER OPERATOR FAMILY operations and acts as a prerequisite validation step
- The function uses a 3-parameter system cache lookup (OPFAMILYAMNAMENSP) which indexes on access method OID, family name, and namespace OID
- Error messages are user-friendly and include all relevant identifying information (family name, access method name, and schema name)
- The function follows PostgreSQL's pattern of immediate error reporting rather than returning boolean status values
- Located in src/backend/commands/opclasscmds.c:1828-1842