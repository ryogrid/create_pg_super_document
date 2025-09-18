# has_tablespace_privilege_id_name

## Location
src/backend/utils/adt/acl.c: 4315 - 4337

## Overview
PostgreSQL built-in function that checks whether a specified user (by role OID) has specific privileges on a tablespace identified by name.

## Definition
```c
Datum has_tablespace_privilege_id_name(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the SQL function `has_tablespace_privilege(role_oid, tablespace_name, privilege)` where the user is specified by role OID, the tablespace by name, and the privilege as a text string. It performs privilege checking by:

1. Converting the tablespace name to its corresponding OID
2. Converting the privilege string to an `AclMode` representation
3. Performing a standard ACL check using the provided role OID
4. Returning a boolean result indicating whether the privilege check succeeded

This variant combines the efficiency of OID-based user identification with name-based tablespace lookup, using the standard ACL checking method rather than the extended version.

## Parameters / Member Variables
- `roleid` (Oid): Object identifier of the user/role to check privileges for
- `tablespacename` (text): Name of the tablespace to check privileges for
- `priv_type_text` (text): Privilege type as a string (e.g., 'CREATE', 'ALL')

## Dependencies
- Functions called/Symbols referenced:
  - convert_tablespace_name (converts tablespace name to OID)
  - convert_tablespace_priv_string (converts privilege string to AclMode)
  - object_aclcheck (performs the standard privilege check)
- Called from (representative examples):
  - No direct references found (called via SQL function dispatch)

## Notes and Other Information
- Uses standard ACL checking (not extended), so will throw an error if tablespace doesn't exist
- Efficient for cases where the role OID is already known
- Part of the overloaded `has_tablespace_privilege` function family
- Located in `src/backend/utils/adt/acl.c:4315-4337`
- Does not handle missing tablespace objects gracefully (unlike extended variants)