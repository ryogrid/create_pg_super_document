# pg_attribute_aclcheck_all

## Location
src/backend/catalog/aclchk.c: 3967 - 3977

## Overview
This function checks a user's access privileges to any or all columns in a table, depending on the specified checking mode.

## Definition
```c
AclResult pg_attribute_aclcheck_all(Oid table_oid, Oid roleid, AclMode mode, AclMaskHow how)
```

## Detailed Description
The `pg_attribute_aclcheck_all` function is an exported routine that verifies whether a specified user (role) has the requested access privileges to columns within a table. The function supports two checking modes: ACLMASK_ANY (user must have privileges on at least one non-dropped column) and ACLMASK_ALL (user must have privileges on all non-dropped columns). This function is a wrapper that calls the extended version `pg_attribute_aclcheck_all_ext` with a NULL parameter for missing attribute detection. System columns are explicitly not considered in this check.

## Parameters / Member Variables
- `table_oid`: The OID of the table containing the columns to be checked
- `roleid`: The OID of the role (user) whose privileges are being checked  
- `mode`: The access mode being requested (e.g., ACL_SELECT, ACL_UPDATE)
- `how`: Specifies checking mode - ACLMASK_ANY (any column) or ACLMASK_ALL (all columns)

## Dependencies
- Functions called/Symbols referenced:
  - pg_attribute_aclcheck_all_ext
  - AclMaskHow
  - AclResult
- Called from (representative examples):
  - ExecCheckOneRelPerms
  - ExecCheckPermissionsModified
  - has_any_column_privilege_name_name
  - has_any_column_privilege_name
  - has_any_column_privilege_id_name
  - all_rows_selectable

## Notes and Other Information
- Part of PostgreSQL's column-level access control system for checking multiple columns at once
- System columns are intentionally excluded from the privilege checking
- Only considers privileges granted directly on the columns, not inherited privileges
- ACLMASK_ANY mode requires at least one accessible column, ACLMASK_ALL mode requires all columns to be accessible
- Returns ACLCHECK_OK if privilege requirements are met, ACLCHECK_NO_PRIV otherwise
- Located in src/backend/catalog/aclchk.c lines 3967-3977