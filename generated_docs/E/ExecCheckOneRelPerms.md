# ExecCheckOneRelPerms

## Location
src/backend/executor/execMain.c: 636 - 744

## Overview
Checks access permissions for a single relation, ensuring that the current user has the required permissions at either the relation level or column level to perform the specified operations.

## Definition


## Detailed Description
This function performs comprehensive permission checking for a single relation by examining both relation-level and column-level permissions. It starts by checking if the user has the required permissions at the relation level using pg_class_aclmask(). If some permissions are missing at the relation level, it then checks if those permissions can be satisfied at the column level for SELECT, INSERT, and UPDATE operations.

The function handles special cases such as:
- Queries that don't reference specific columns (e.g., SELECT COUNT(*)) by requiring SELECT permission on at least one column
- Whole-row references that require permissions on all columns
- Column-specific permissions for INSERT and UPDATE operations

The user ID for permission checking is determined either from the checkAsUser field in the RTEPermissionInfo structure (for setuid operations) or defaults to the current user.

## Parameters / Member Variables
- : Pointer to RTEPermissionInfo structure containing:
  - : OID of the relation to check permissions for
  - : Bitmask of required permissions (ACL_SELECT, ACL_INSERT, ACL_UPDATE, etc.)
  - : Optional user ID to check permissions as (for setuid operations)
  - : Bitmap of columns referenced in SELECT operations
  - : Bitmap of columns referenced in INSERT operations
  - : Bitmap of columns referenced in UPDATE operations

## Dependencies
- Functions called/Symbols referenced:
  - [pg_class_aclmask](../p/pg_class_aclmask.md)
  - [pg_attribute_aclcheck_all](../p/pg_attribute_aclcheck_all.md)
  - [pg_attribute_aclcheck](../p/pg_attribute_aclcheck.md)
  - [ExecCheckPermissionsModified](ExecCheckPermissionsModified.md)
  - bms_is_empty
  - [bms_next_member](../b/bms_next_member.md)
  - [GetUserId](../G/GetUserId.md)
  - OidIsValid
- Called from (representative examples):
  - [ExecCheckPermissions](ExecCheckPermissions.md)
  - [subquery_planner](../s/subquery_planner.md)

## Notes and Other Information
- Returns true if all required permissions are satisfied, false otherwise
- The function optimizes by first checking relation-level permissions before falling back to column-level checks
- Only SELECT, INSERT, and UPDATE permissions can be satisfied at the column level; other permissions (like DELETE, TRUNCATE) must be granted at the relation level
- Error reporting is done at the table level even when column-level permission checks fail
- The function is designed to handle both explicit column references and implicit whole-row references