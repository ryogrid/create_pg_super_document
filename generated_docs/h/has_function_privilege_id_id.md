# has_function_privilege_id_id

## Location
[src/backend/utils/adt/acl.c:3527-3555](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L3527-L3555)

## Overview
Checks whether a specified user has specific privileges on a function identified by its OID.

## Definition
```c
Datum has_function_privilege_id_id(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL built-in function that verifies if a specified user (by role ID) has the specified privilege on a given function (by OID). It takes a role OID, function OID, and privilege name as text, then performs the privilege check. This function combines aspects of the other two has_function_privilege functions: it allows checking privileges for any user (like has_function_privilege_id_name) while using function OIDs directly (like has_function_privilege_id). It properly handles missing functions by returning NULL.

## Parameters / Member Variables
- `roleid` (PG_GETARG_OID(0)): The OID of the role/user to check privileges for
- `functionoid` (PG_GETARG_OID(1)): The OID of the function to check privileges for
- `priv_type_text` (PG_GETARG_TEXT_PP(2)): Text representation of the privilege type to check (e.g., 'EXECUTE')

## Dependencies
- Functions called/Symbols referenced:
  - [convert_function_priv_string](../c/convert_function_priv_string.md)(): Converts privilege string to AclMode bitmask
  - [object_aclcheck_ext](../o/object_aclcheck_ext.md)(): Performs the actual privilege check against the ACL
  - AclResult: Enum type for ACL check results
- Called from (representative examples):
  - No direct callers found (likely called via SQL function interface)

## Notes and Other Information
- This is one of the PostgreSQL privilege checking functions available in SQL
- Returns NULL if the specified function doesn't exist (is_missing flag)
- Uses object_aclcheck_ext which provides better error handling for missing objects
- Most comprehensive of the three has_function_privilege variants (role ID + function OID + privilege)
- Part of the has_*_privilege family of functions for access control verification
- Located in src/backend/utils/adt/acl.c:3527-3555