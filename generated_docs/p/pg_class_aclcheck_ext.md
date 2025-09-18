# pg_class_aclcheck_ext

## Location
src/backend/catalog/aclchk.c: 4106 - 4120

## Overview
This function checks a user's access privileges to a table with extended support for detecting missing relations.

## Definition
```c
AclResult pg_class_aclcheck_ext(Oid table_oid, Oid roleid, AclMode mode, bool *is_missing)
```

## Detailed Description
The `pg_class_aclcheck_ext` function is an extended version of table-level privilege checking that verifies whether a specified user (role) has the requested access privileges to a table or other relation. This function provides additional error handling capabilities by distinguishing between missing relations and permission failures through the is_missing parameter. It internally uses `pg_class_aclmask_ext` to perform the actual privilege checking and returns a simple ACLCHECK_OK or ACLCHECK_NO_PRIV result.

## Parameters / Member Variables
- `table_oid`: The OID of the table/relation to check privileges for
- `roleid`: The OID of the role (user) whose privileges are being checked
- `mode`: The access mode being requested (e.g., ACL_SELECT, ACL_INSERT, ACL_UPDATE, ACL_DELETE)
- `is_missing`: Output parameter that indicates whether the relation was found to be missing

## Dependencies
- Functions called/Symbols referenced:
  - pg_class_aclmask_ext
  - ACLMASK_ANY
  - ACLCHECK_NO_PRIV
  - AclResult
- Called from (representative examples):
  - pg_class_aclcheck
  - has_table_privilege_name_id
  - has_table_privilege_id
  - has_sequence_privilege_name_id
  - has_sequence_privilege_id
  - has_any_column_privilege_name_id
  - column_privilege_check

## Notes and Other Information
- This is the extended interface for table-level privilege checking in PostgreSQL
- Provides better error reporting by distinguishing between permission denied and non-existent relations
- Used by SQL privilege-checking functions that need to handle missing relations gracefully
- Returns ACLCHECK_OK if any of the requested privileges are granted, ACLCHECK_NO_PRIV otherwise
- The extended version is particularly useful for SQL functions that should return NULL instead of throwing errors for missing objects
- Located in src/backend/catalog/aclchk.c lines 4106-4120