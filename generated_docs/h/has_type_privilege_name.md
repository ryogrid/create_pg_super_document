# has_type_privilege_name

## Location
[src/backend/utils/adt/acl.c:4432-4455](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L4432-L4455)

## Overview
Checks whether the current user has a particular privilege on a specified type, using the typename as a text string.

## Definition
```c
Datum has_type_privilege_name(PG_FUNCTION_ARGS)
```

## Detailed Description
This PostgreSQL function provides a convenient way to check type privileges for the current user without explicitly specifying a username. It is part of the has_type_privilege family of functions and assumes the privilege check should be performed for the currently authenticated user. The function takes a type name and privilege specification as text arguments, converts them to internal identifiers, and performs the privilege check through PostgreSQL's access control system.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS[0]`: Type name as text (the target type for privilege checking)
- `PG_FUNCTION_ARGS[1]`: Privilege specification as text (e.g., "USAGE")

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP
  - [GetUserId](../G/GetUserId.md)
  - [convert_type_name](../c/convert_type_name.md)
  - [convert_type_priv_string](../c/convert_type_priv_string.md)
  - [object_aclcheck](../o/object_aclcheck.md)
  - PG_RETURN_BOOL
- Called from (representative examples):
  - This function is callable from SQL as has_type_privilege()

## Notes and Other Information
- This is a PostgreSQL SQL-callable function that returns a boolean result
- Automatically uses the current user's ID via GetUserId(), making it convenient for self-privilege checks
- Returns true if the current user has the specified privilege, false if not, or NULL if the object doesn't exist
- Part of the has_type_privilege family with different parameter combinations
- Uses object_aclcheck with TypeRelationId to perform the actual privilege verification
- Commonly used in application code where the current user's permissions need to be verified