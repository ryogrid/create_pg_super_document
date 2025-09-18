# has_foreign_data_wrapper_privilege_name_id

## Location
[src/backend/utils/adt/acl.c:3246-3275](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L3246-L3275)

## Overview
Checks if a specified user has a particular privilege on a foreign data wrapper identified by its OID.

## Definition
```c
Datum has_foreign_data_wrapper_privilege_name_id(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL SQL-callable function that verifies whether a specific user has a given privilege on a foreign data wrapper. Unlike the previous function, this variant takes three arguments: a username (as Name type), the foreign data wrapper's OID, and the privilege type as text. The function resolves the username to a role OID using get_role_oid_or_public(), converts the privilege string to an AclMode, and performs an extended privilege check that can detect missing objects. If the foreign data wrapper doesn't exist, the function returns NULL; otherwise, it returns a boolean indicating whether the privilege is granted.

## Parameters / Member Variables
- `username` (Name): The name of the user/role to check privileges for
- `fdwid` (Oid): The OID of the foreign data wrapper to check privileges on
- `priv_type_text` (text): The privilege type to check (e.g., 'USAGE')

## Dependencies
- Functions called/Symbols referenced:
  - get_role_oid_or_public (converts username to role OID, handles 'public' role)
  - [convert_foreign_data_wrapper_priv_string](../c/convert_foreign_data_wrapper_priv_string.md) (converts privilege string to AclMode)
  - [object_aclcheck_ext](../o/object_aclcheck_ext.md) (performs extended privilege check with missing object detection)
  - PG_GETARG_NAME (macro to extract Name argument)
  - PG_GETARG_OID (macro to extract OID argument)
  - PG_GETARG_TEXT_PP (macro to extract text argument)
  - PG_RETURN_NULL (macro to return NULL)
  - PG_RETURN_BOOL (macro to return boolean result)
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- Returns NULL if the specified foreign data wrapper doesn't exist (is_missing flag)
- Uses object_aclcheck_ext() instead of object_aclcheck() to handle missing objects gracefully
- Supports checking privileges for any user, not just the current user
- Part of PostgreSQL's comprehensive access control system for foreign data wrappers
- Located in src/backend/utils/adt/acl.c:3246-3275