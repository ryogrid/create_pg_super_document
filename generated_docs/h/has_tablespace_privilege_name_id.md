# has_tablespace_privilege_name_id

## Location
src/backend/utils/adt/acl.c: 4257 - 4286

## Overview
PostgreSQL built-in function that checks whether a specified user (by name) has specific privileges on a tablespace identified by OID.

## Definition
```c
Datum has_tablespace_privilege_name_id(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the SQL function `has_tablespace_privilege(user, tablespace_oid, privilege)` where the user is specified by name, the tablespace by OID, and the privilege as a text string. It performs privilege checking by:

1. Converting the username to a role OID using `get_role_oid_or_public`
2. Converting the privilege string to an `AclMode` representation
3. Performing an extended ACL check that can detect missing objects
4. Returning NULL if the tablespace doesn't exist, or boolean result otherwise

This variant uses the extended ACL check (`object_aclcheck_ext`) which provides better error handling by distinguishing between permission denied and object not found scenarios.

## Parameters / Member Variables
- `username` (Name): Name of the user/role to check privileges for
- `tablespaceoid` (Oid): Object identifier of the tablespace
- `priv_type_text` (text): Privilege type as a string (e.g., 'CREATE', 'ALL')

## Dependencies
- Functions called/Symbols referenced:
  - get_role_oid_or_public (converts username to role OID, handles 'public' role)
  - convert_tablespace_priv_string (converts privilege string to AclMode)
  - object_aclcheck_ext (performs extended privilege check with missing object detection)
- Called from (representative examples):
  - No direct references found (called via SQL function dispatch)

## Notes and Other Information
- Returns NULL if the tablespace object is missing (rather than throwing an error)
- Uses extended ACL checking for better error handling compared to basic variants
- Part of the overloaded `has_tablespace_privilege` function family
- Located in `src/backend/utils/adt/acl.c:4257-4286`
- The `is_missing` flag allows graceful handling of non-existent tablespaces