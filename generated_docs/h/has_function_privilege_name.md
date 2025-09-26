# has_function_privilege_name

## Location
[src/backend/utils/adt/acl.c:3422-3445](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L3422-L3445)

## Overview
Checks whether the current user has specific privileges on a function, using function name and privilege type as parameters.

## Definition
Datum has_function_privilege_name(PG_FUNCTION_ARGS)

## Detailed Description
This function is another variant of the PostgreSQL built-in "has_function_privilege" function available at the SQL level. Unlike the three-parameter version, this variant assumes the current user and only requires the function name and privilege type as parameters. It's a convenience function that eliminates the need to explicitly specify the current user when checking one's own privileges.

The function follows a streamlined privilege checking process:
1. Gets the current user's OID using GetUserId()
2. Resolves the function name to its corresponding object OID
3. Converts the privilege string to an AclMode bitmask
4. Performs the privilege check using the object access control system
5. Returns a boolean result indicating success or failure

This variant is commonly used in SQL queries where users want to check their own privileges without needing to know their username or user ID.

## Parameters / Member Variables
- Function uses PG_FUNCTION_ARGS macro, extracting:
  -  (text): The name of the function to check privileges on
  -  (text): The privilege type string (e.g., "EXECUTE")

## Dependencies
- Functions called/Symbols referenced:
  - [AclResult](../A/AclResult.md): Enumeration for access control check results
  - [GetUserId](../G/GetUserId.md): Gets the current user's OID from the session context
  - [convert_function_name](../c/convert_function_name.md): Converts function name text to function OID
  - [convert_function_priv_string](../c/convert_function_priv_string.md): Converts privilege string to AclMode bitmask
  - [object_aclcheck](../o/object_aclcheck.md): Performs the actual access control check
- Called from (representative examples):
  - No direct references found (likely called through SQL interface)

## Notes and Other Information
- This is a PostgreSQL built-in function accessible via SQL as has_function_privilege(functionname, privilege)
- Part of the has_function_privilege family providing different parameter combinations for user convenience
- Uses GetUserId() to automatically determine the current user, eliminating the need for explicit user specification
- Returns BOOL type to SQL layer (true/false/NULL)
- Uses the same privilege checking infrastructure as other variants but with implicit user context
- Commonly used in application code and SQL scripts where self-privilege checking is needed
- The function will return NULL if the specified function doesn't exist
- More concise than the name_name variant when checking current user privileges