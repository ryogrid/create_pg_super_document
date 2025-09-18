# has_tablespace_privilege_id_id

## Location
src/backend/utils/adt/acl.c: 4338 - 4366

## Overview
PostgreSQL built-in function that checks whether a specified user (by role OID) has specific privileges on a tablespace identified by OID.

## Definition
```c
Datum has_tablespace_privilege_id_id(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the SQL function `has_tablespace_privilege(role_oid, tablespace_oid, privilege)` where both the user and tablespace are specified by their respective OIDs, and the privilege as a text string. This is the most efficient variant as it avoids any name-to-OID conversions. The function operates by:

1. Converting only the privilege string to an `AclMode` representation
2. Performing an extended ACL check that can detect missing tablespaces
3. Returning NULL if the tablespace doesn't exist, or boolean result otherwise

This variant combines maximum efficiency (both user and tablespace identified by OID) with robust error handling through extended ACL checking.

## Parameters / Member Variables
- `roleid` (Oid): Object identifier of the user/role to check privileges for
- `tablespaceoid` (Oid): Object identifier of the tablespace to check privileges for
- `priv_type_text` (text): Privilege type as a string (e.g., 'CREATE', 'ALL')

## Dependencies
- Functions called/Symbols referenced:
  - convert_tablespace_priv_string (converts privilege string to AclMode)
  - object_aclcheck_ext (performs extended privilege check with missing object detection)
- Called from (representative examples):
  - No direct references found (called via SQL function dispatch)

## Notes and Other Information
- Most efficient variant as it uses OIDs for both user and tablespace identification
- Returns NULL if the tablespace object is missing rather than throwing an error
- Uses extended ACL checking for better error handling
- Part of the overloaded `has_tablespace_privilege` function family
- Located in `src/backend/utils/adt/acl.c:4338-4366`
- Optimal for performance-critical code paths where OIDs are already available