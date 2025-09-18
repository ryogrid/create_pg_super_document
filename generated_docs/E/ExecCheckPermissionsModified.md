# ExecCheckPermissionsModified

## Location
src/backend/executor/execMain.c: 745 - 791

## Overview
Checks INSERT or UPDATE access permissions for a single relation at the column level, processing both operation types uniformly.

## Definition


## Detailed Description
This static function is a specialized permission checker for INSERT and UPDATE operations that require column-level permission verification. It iterates through the bitmap of modified columns and checks that the specified user has the required permissions on each individual column. The function handles special cases such as operations that don't explicitly specify columns (e.g., SELECT FOR UPDATE) by requiring permission on at least one column of the relation.

The function prevents whole-row updates by explicitly checking for InvalidAttrNumber and throwing an error if encountered, as whole-row updates are not implemented in PostgreSQL.

## Parameters / Member Variables
- : OID of the relation being checked
- : User ID to check permissions for
- : Bitmap of columns that are being modified (inserted or updated)
- : The specific permission being checked (ACL_INSERT or ACL_UPDATE)

## Dependencies
- Functions called/Symbols referenced:
  - bms_is_empty
  - [bms_next_member](../b/bms_next_member.md)
  - [pg_attribute_aclcheck_all](../p/pg_attribute_aclcheck_all.md)
  - [pg_attribute_aclcheck](../p/pg_attribute_aclcheck.md)
  - elog
- Called from (representative examples):
  - [ExecCheckOneRelPerms](ExecCheckOneRelPerms.md) (for both INSERT and UPDATE permission checks)

## Notes and Other Information
- This is a static function only called from within execMain.c
- Returns true if all required permissions are satisfied, false otherwise
- Handles the case where no columns are explicitly modified by requiring permission on any column
- Uses bitmap iteration to efficiently process large column sets
- The function explicitly prevents whole-row updates by throwing an ERROR
- Column numbers are offset by FirstLowInvalidHeapAttributeNumber to handle system columns
- Designed to work uniformly for both INSERT and UPDATE operations