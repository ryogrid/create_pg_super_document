# has_type_privilege_name_name

## Location
src/backend/utils/adt/acl.c: 4406 - 4431

## Overview
Checks whether a specified user has a particular privilege on a type, where both the username and typename are provided as text strings.

## Definition
```c
Datum has_type_privilege_name_name(PG_FUNCTION_ARGS)
```

## Detailed Description
This PostgreSQL function implements privilege checking for database types using string-based identifiers for both the user and the type. It serves as one of the variants in the has_type_privilege family of functions that check type-level permissions. The function takes three arguments: a username, a type name, and a privilege specification, then performs the necessary conversions from text to internal identifiers before conducting the actual privilege check through PostgreSQL's access control system.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS[0]`: Username as a Name type (the user whose privileges are being checked)
- `PG_FUNCTION_ARGS[1]`: Type name as text (the target type for privilege checking)
- `PG_FUNCTION_ARGS[2]`: Privilege specification as text (e.g., "USAGE")

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NAME
  - PG_GETARG_TEXT_PP
  - get_role_oid_or_public
  - [convert_type_name](../c/convert_type_name.md)
  - [convert_type_priv_string](../c/convert_type_priv_string.md)
  - [object_aclcheck](../o/object_aclcheck.md)
  - PG_RETURN_BOOL
- Called from (representative examples):
  - This function is callable from SQL as has_type_privilege()

## Notes and Other Information
- This is a PostgreSQL SQL-callable function that returns a boolean result
- Returns true if the user has the specified privilege, false if not, or NULL if the object doesn't exist
- Part of the has_type_privilege family of functions with different parameter combinations
- Uses object_aclcheck with TypeRelationId to perform the actual privilege verification
- The function handles both regular users and the special "public" pseudo-role
- Follows PostgreSQL's standard pattern for privilege checking functions