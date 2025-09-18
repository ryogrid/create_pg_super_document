# has_foreign_data_wrapper_privilege_id_id

## Location
src/backend/utils/adt/acl.c: 3327 - 3355

## Overview
Checks if a specified role (by OID) has a particular privilege on a foreign data wrapper (by OID).

## Definition
```c
Datum has_foreign_data_wrapper_privilege_id_id(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL SQL-callable function that verifies whether a specific role has a given privilege on a foreign data wrapper. This is the most efficient variant of the foreign data wrapper privilege checking functions, as it takes both the role OID and foreign data wrapper OID directly as arguments, avoiding any name-to-OID conversions. It takes three arguments: a role OID, the foreign data wrapper OID, and the privilege type as text. The function converts only the privilege string to an AclMode and uses the extended object access control checking mechanism. If the foreign data wrapper doesn't exist, the function returns NULL; otherwise, it returns a boolean indicating the privilege status.

## Parameters / Member Variables
- `roleid` (Oid): The OID of the role/user to check privileges for
- `fdwid` (Oid): The OID of the foreign data wrapper to check privileges on
- `priv_type_text` (text): The privilege type to check (e.g., 'USAGE')

## Dependencies
- Functions called/Symbols referenced:
  - [convert_foreign_data_wrapper_priv_string](../c/convert_foreign_data_wrapper_priv_string.md) (converts privilege string to AclMode)
  - [object_aclcheck_ext](../o/object_aclcheck_ext.md) (performs extended privilege check with missing object detection)
  - PG_GETARG_OID (macro to extract OID arguments)
  - PG_GETARG_TEXT_PP (macro to extract text argument)
  - PG_RETURN_NULL (macro to return NULL)
  - PG_RETURN_BOOL (macro to return boolean result)
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- Most efficient variant as it uses OIDs directly for both role and foreign data wrapper
- Returns NULL if the specified foreign data wrapper doesn't exist (is_missing flag)
- Uses object_aclcheck_ext() for comprehensive error handling
- No name resolution required, making it the fastest of the privilege checking variants
- Part of PostgreSQL's access control system for foreign data wrappers
- Located in src/backend/utils/adt/acl.c:3327-3355