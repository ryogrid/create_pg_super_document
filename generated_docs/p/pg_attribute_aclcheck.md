# pg_attribute_aclcheck

## Location
[src/backend/catalog/aclchk.c:3925-3936](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L3925-L3936)

## Overview
This function provides a public interface for checking a user's access privileges to a specific table column (attribute) in PostgreSQL.

## Definition
AclResult pg_attribute_aclcheck(Oid table_oid, AttrNumber attnum, Oid roleid, AclMode mode)

## Detailed Description
This is a wrapper function that provides column-level access control checking in PostgreSQL. It focuses specifically on privileges granted directly to individual columns, not inherited table-level privileges. The function delegates to pg_attribute_aclcheck_ext with NULL for the is_missing parameter, meaning it will throw errors rather than handling missing tables/columns gracefully. Column-level privileges are particularly important for fine-grained security in PostgreSQL where different users may need access to different columns of the same table.

## Parameters / Member Variables
- table_oid: The OID of the table containing the column being checked
- attnum: The attribute number (column number) being checked for permissions
- roleid: The OID of the role whose permissions are being verified
- mode: The access mode/permissions being requested (typically ACL_SELECT, ACL_UPDATE, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_attribute_aclcheck_ext](pg_attribute_aclcheck_ext.md)
- Called from (representative examples):
  - [BuildIndexValueDescription](../B/BuildIndexValueDescription.md)
  - [checkFkeyPermissions](../c/checkFkeyPermissions.md)
  - [ExecCheckOneRelPerms](../E/ExecCheckOneRelPerms.md)
  - [ExecCheckPermissionsModified](../E/ExecCheckPermissionsModified.md)
  - [ExecBuildSlotValueDescription](../E/ExecBuildSlotValueDescription.md)
  - [ri_ReportViolation](../r/ri_ReportViolation.md)
  - [all_rows_selectable](../a/all_rows_selectable.md)

## Notes and Other Information  
- Only considers privileges granted directly to the specific column, not table-level privileges
- Returns ACLCHECK_OK if the user has any of the requested privileges, ACLCHECK_NO_PRIV otherwise
- Used extensively in execution engine for runtime permission checking on column access
- Critical for implementing PostgreSQL's column-level security model
- Unlike table-level checking, column privileges are more granular and require the specific attribute number
- Function is exported and declared in src/include/utils/acl.h for use throughout the system