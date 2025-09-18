# has_type_privilege_id

## Location
src/backend/utils/adt/acl.c: 4486 - 4513

## Overview
Checks user privileges on a type given a type OID and text privilege name, assuming the current user context.

## Definition
```c
Datum has_type_privilege_id(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL built-in function that checks whether the current user has specific privileges on a given type. It takes a type OID and a privilege string as arguments, converts the privilege string to the appropriate access control mode, and performs an access control check using the object access control framework. The function returns a boolean result indicating whether the privilege check succeeded or NULL if the type is missing.

## Parameters / Member Variables
- `typeoid` (PG_GETARG_OID(0)): The object identifier of the type to check privileges for
- `priv_type_text` (PG_GETARG_TEXT_PP(1)): Text representation of the privilege(s) to check (e.g., "USAGE")

## Dependencies
- Functions called/Symbols referenced:
  - [GetUserId](../G/GetUserId.md): Gets the current user's OID
  - [convert_type_priv_string](../c/convert_type_priv_string.md): Converts privilege text to AclMode bitmask
  - [object_aclcheck_ext](../o/object_aclcheck_ext.md): Performs the actual access control check
  - PG_RETURN_NULL: Returns NULL if type is missing
  - PG_RETURN_BOOL: Returns boolean result of privilege check
- Called from (representative examples):
  - No direct callers found (likely called via SQL function interface)

## Notes and Other Information
- This function assumes the current user context (uses GetUserId())
- Returns NULL if the specified type does not exist (is_missing flag)
- Part of PostgreSQL's access control system for type privileges
- Typically used in SQL queries like: SELECT has_type_privilege('mytype', 'USAGE')
- Located in src/backend/utils/adt/acl.c:4486-4513