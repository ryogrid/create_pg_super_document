# pg_class_aclcheck

## Location
src/backend/catalog/aclchk.c: 4096 - 4105

## Overview
This function checks a user's access privileges to a table or relation.

## Definition
```c
AclResult pg_class_aclcheck(Oid table_oid, Oid roleid, AclMode mode)
```

## Detailed Description
The `pg_class_aclcheck` function is an exported routine that verifies whether a specified user (role) has the requested access privileges to a table or other relation. This function serves as a wrapper around the extended version `pg_class_aclcheck_ext`, providing a simpler interface when missing relation detection is not needed. It returns ACLCHECK_OK if the user has any of the requested privileges, otherwise returns ACLCHECK_NO_PRIV.

## Parameters / Member Variables
- `table_oid`: The OID of the table/relation to check privileges for
- `roleid`: The OID of the role (user) whose privileges are being checked
- `mode`: The access mode being requested (e.g., ACL_SELECT, ACL_INSERT, ACL_UPDATE, ACL_DELETE)

## Dependencies
- Functions called/Symbols referenced:
  - pg_class_aclcheck_ext
  - AclResult
- Called from (representative examples):
  - BuildIndexValueDescription
  - cluster_is_permitted_for_relation
  - RangeVarCallbackForReindexIndex
  - LockTableAclCheck
  - nextval_internal
  - truncate_check_perms
  - CreateTriggerFiringOn
  - vacuum_is_permitted_for_relation
  - has_table_privilege_name_name
  - has_sequence_privilege_name_name
  - has_any_column_privilege_name_name

## Notes and Other Information
- This is the standard interface for table-level privilege checking in PostgreSQL
- Used extensively throughout the system for checking table access permissions
- Covers various relation types including regular tables, sequences, indexes, etc.
- Returns ACLCHECK_OK for success, ACLCHECK_NO_PRIV for insufficient privileges
- Located in src/backend/catalog/aclchk.c lines 4096-4105
- This wrapper function simplifies calls when missing relation detection is not required