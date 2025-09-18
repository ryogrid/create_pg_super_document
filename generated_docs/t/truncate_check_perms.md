# truncate_check_perms

## Location
src/backend/commands/tablecmds.c: 2350 - 2367

## Overview
truncate_check_perms verifies that the current user has the necessary TRUNCATE permission on a given relation, raising an appropriate error if access is denied.

## Definition
```c
static void truncate_check_perms(Oid relid, Form_pg_class reltuple)
```

## Detailed Description
This function performs a straightforward but critical permission check for TRUNCATE operations:

1. **Permission Verification**: Uses PostgreSQL's Access Control List (ACL) system to check if the current user has TRUNCATE privileges on the specified relation

2. **Error Reporting**: If the permission check fails, generates an appropriate error message using the relation's name and type, providing clear feedback to the user about the access denial

The function is designed to be simple and focused, handling only the permission aspect of truncate validation. It integrates with PostgreSQL's broader security framework by using the standard ACL checking mechanisms.

## Parameters / Member Variables
- `relid`: Object ID of the relation to check permissions for
- `reltuple`: Form_pg_class tuple containing the relation's catalog information, used for extracting the relation name and type for error messages

## Dependencies
- Functions called/Symbols referenced:
  - [pg_class_aclcheck](../p/pg_class_aclcheck.md)
  - [aclcheck_error](../a/aclcheck_error.md)  
  - [get_relkind_objtype](../g/get_relkind_objtype.md)
- Called from (representative examples):
  - [ExecuteTruncateGuts](../E/ExecuteTruncateGuts.md)
  - [RangeVarCallbackForTruncate](../R/RangeVarCallbackForTruncate.md)

## Notes and Other Information
- This function specifically checks for ACL_TRUNCATE privilege, which is distinct from other table privileges like SELECT, INSERT, UPDATE, or DELETE
- The TRUNCATE privilege can be granted independently of other table privileges, allowing for fine-grained access control
- The function is typically called after truncate_check_rel() to ensure both the relation type is valid and the user has appropriate permissions
- For inherited TRUNCATE operations, permission checks are only performed on the parent table, not on child tables
- The error message generated includes the specific relation type (table, foreign table, etc.) to provide context-appropriate feedback