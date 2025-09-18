# has_foreign_data_wrapper_privilege_id

## Location
src/backend/utils/adt/acl.c: 3276 - 3303

## Overview
Checks if the current user has a specified privilege on a foreign data wrapper identified by its OID.

## Definition
```c
Datum has_foreign_data_wrapper_privilege_id(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL SQL-callable function that verifies whether the current user has a specific privilege on a foreign data wrapper. It takes two arguments: the foreign data wrapper's OID and the privilege type as text. The function uses the current session's user ID (obtained via GetUserId()) to perform the privilege check. It converts the privilege string to an AclMode and uses the extended object access control checking mechanism to determine if the privilege is granted. If the foreign data wrapper doesn't exist, the function returns NULL; otherwise, it returns a boolean indicating the privilege status.

## Parameters / Member Variables
- `fdwid` (Oid): The OID of the foreign data wrapper to check privileges on
- `priv_type_text` (text): The privilege type to check (e.g., 'USAGE')

## Dependencies
- Functions called/Symbols referenced:
  - GetUserId (to get current user's OID)
  - convert_foreign_data_wrapper_priv_string (converts privilege string to AclMode)
  - object_aclcheck_ext (performs extended privilege check with missing object detection)
  - PG_GETARG_OID (macro to extract OID argument)
  - PG_GETARG_TEXT_PP (macro to extract text argument)
  - PG_RETURN_NULL (macro to return NULL)
  - PG_RETURN_BOOL (macro to return boolean result)
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- This function assumes the current user (GetUserId()) as the subject of the privilege check
- Returns NULL if the specified foreign data wrapper doesn't exist (is_missing flag)
- Uses object_aclcheck_ext() instead of object_aclcheck() for better error handling
- More efficient than name-based lookup since it directly uses the OID
- Part of PostgreSQL's access control system for foreign data wrappers
- Located in src/backend/utils/adt/acl.c:3276-3303