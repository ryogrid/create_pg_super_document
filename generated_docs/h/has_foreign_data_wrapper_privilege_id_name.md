# has_foreign_data_wrapper_privilege_id_name

## Location
src/backend/utils/adt/acl.c: 3304 - 3326

## Overview
Checks if a specified role (by OID) has a particular privilege on a foreign data wrapper identified by name.

## Definition
```c
Datum has_foreign_data_wrapper_privilege_id_name(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL SQL-callable function that verifies whether a specific role has a given privilege on a foreign data wrapper. It takes three arguments: a role OID, the foreign data wrapper name as text, and the privilege type as text. The function converts the foreign data wrapper name to its OID using convert_foreign_data_wrapper_name(), converts the privilege string to an AclMode, and performs a standard privilege check. This variant is useful when you have the role OID directly but need to look up the foreign data wrapper by name.

## Parameters / Member Variables
- `roleid` (Oid): The OID of the role/user to check privileges for
- `fdwname` (text): The name of the foreign data wrapper to check privileges on
- `priv_type_text` (text): The privilege type to check (e.g., 'USAGE')

## Dependencies
- Functions called/Symbols referenced:
  - convert_foreign_data_wrapper_name (converts text name to OID)
  - convert_foreign_data_wrapper_priv_string (converts privilege string to AclMode)
  - object_aclcheck (performs the privilege check)
  - PG_GETARG_OID (macro to extract OID argument)
  - PG_GETARG_TEXT_PP (macro to extract text arguments)
  - PG_RETURN_BOOL (macro to return boolean result)
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- Takes role OID as direct input, bypassing username-to-OID conversion
- Requires foreign data wrapper name lookup, which may be less efficient than OID-based variants
- Uses object_aclcheck() instead of object_aclcheck_ext(), so doesn't handle missing objects gracefully
- Part of PostgreSQL's comprehensive access control system for foreign data wrappers
- Located in src/backend/utils/adt/acl.c:3304-3326