# has_function_privilege_id_name

## Location
[src/backend/utils/adt/acl.c:3504-3526](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L3504-L3526)

## Overview
Checks whether a specified user has specific privileges on a function identified by its name.

## Definition
```c
Datum has_function_privilege_id_name(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL built-in function that verifies if a specified user (by role ID) has the specified privilege on a given function (by name). It takes a role OID, function name as text, and privilege name as text, then performs the privilege check. Unlike has_function_privilege_id, this function allows checking privileges for any user, not just the current user, and accepts a function name instead of an OID.

## Parameters / Member Variables
- `roleid` (PG_GETARG_OID(0)): The OID of the role/user to check privileges for
- `functionname` (PG_GETARG_TEXT_PP(1)): Text name of the function to check privileges for
- `priv_type_text` (PG_GETARG_TEXT_PP(2)): Text representation of the privilege type to check (e.g., 'EXECUTE')

## Dependencies
- Functions called/Symbols referenced:
  - [convert_function_name](../c/convert_function_name.md)(): Converts function name to function OID
  - [convert_function_priv_string](../c/convert_function_priv_string.md)(): Converts privilege string to AclMode bitmask
  - [object_aclcheck](../o/object_aclcheck.md)(): Performs the actual privilege check against the ACL
  - [AclResult](../A/AclResult.md): Enum type for ACL check results
- Called from (representative examples):
  - No direct callers found (likely called via SQL function interface)

## Notes and Other Information
- This is one of the PostgreSQL privilege checking functions available in SQL
- Unlike has_function_privilege_id, this function does not handle missing functions with NULL return
- Uses object_aclcheck instead of object_aclcheck_ext (no missing object handling)
- Part of the has_*_privilege family of functions for access control verification
- Located in src/backend/utils/adt/acl.c:3504-3526