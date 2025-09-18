# has_type_privilege_id_name

## Location
src/backend/utils/adt/acl.c: 4514 - 4536

## Overview
Checks user privileges on a type given a role ID, type name (as text), and privilege name (as text).

## Definition
```c
Datum has_type_privilege_id_name(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL built-in function that checks whether a specific user (identified by role ID) has specific privileges on a named type. It takes a role OID, a type name as text, and a privilege string as arguments. The function first converts the type name to its corresponding OID, then converts the privilege string to the appropriate access control mode, and finally performs an access control check. Unlike has_type_privilege_id, this variant does not assume current user context and allows checking privileges for any specified role.

## Parameters / Member Variables
- `roleid` (PG_GETARG_OID(0)): The object identifier of the role (user) to check privileges for
- `typename` (PG_GETARG_TEXT_PP(1)): Text name of the type to check privileges for
- `priv_type_text` (PG_GETARG_TEXT_PP(2)): Text representation of the privilege(s) to check (e.g., "USAGE")

## Dependencies
- Functions called/Symbols referenced:
  - [convert_type_name](../c/convert_type_name.md): Converts type name text to type OID
  - [convert_type_priv_string](../c/convert_type_priv_string.md): Converts privilege text to AclMode bitmask
  - [object_aclcheck](../o/object_aclcheck.md): Performs the actual access control check
  - PG_RETURN_BOOL: Returns boolean result of privilege check
- Called from (representative examples):
  - No direct callers found (likely called via SQL function interface)

## Notes and Other Information
- This function allows checking privileges for any role, not just the current user
- Does not handle missing types gracefully (will error if type name is invalid)
- Part of PostgreSQL's access control system for type privileges
- Typically used in SQL queries like: SELECT has_type_privilege('role_name', 'mytype', 'USAGE')
- Located in src/backend/utils/adt/acl.c:4514-4536