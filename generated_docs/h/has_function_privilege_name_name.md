# has_function_privilege_name_name

## Location
src/backend/utils/adt/acl.c: 3396 - 3421

## Overview
Checks whether a user has specific privileges on a function, using role name, function name, and privilege type as text strings.

## Definition
Datum has_function_privilege_name_name(PG_FUNCTION_ARGS)

## Detailed Description
This function is one of the PostgreSQL built-in functions available at the SQL level under the name "has_function_privilege". It determines whether a specified user has particular privileges on a given function. This variant takes all three parameters as text/name types: the username, function name, and privilege type.

The function follows the standard PostgreSQL privilege checking pattern:
1. Converts the username to a role OID, supporting both regular role names and the special "public" role
2. Resolves the function name to its corresponding object OID
3. Converts the privilege string to an AclMode bitmask
4. Performs the actual privilege check using the object access control system
5. Returns a boolean result indicating whether the privilege check succeeded

The function returns NULL if the specified function doesn't exist, true if the user has the privilege, and false otherwise.

## Parameters / Member Variables
- Function uses PG_FUNCTION_ARGS macro, extracting:
  -  (Name): The name of the role to check privileges for
  -  (text): The name of the function to check privileges on  
  -  (text): The privilege type string (e.g., "EXECUTE")

## Dependencies
- Functions called/Symbols referenced:
  - Name: PostgreSQL name type
  - PG_GETARG_NAME: Macro to extract name argument from function call
  - AclResult: Enumeration for access control check results
  - get_role_oid_or_public: Converts role name to OID, handling "public"
  - convert_function_name: Converts function name text to function OID
  - convert_function_priv_string: Converts privilege string to AclMode bitmask
  - object_aclcheck: Performs the actual access control check
- Called from (representative examples):
  - No direct references found (likely called through SQL interface)

## Notes and Other Information
- This is a PostgreSQL built-in function accessible via SQL as has_function_privilege(username, functionname, privilege)
- Part of the has_function_privilege family of functions that provide different parameter combinations
- Uses the ProcedureRelationId constant to indicate that privilege checking is being done on functions/procedures
- Returns BOOL type to SQL layer (true/false/NULL)
- The function handles the "public" role specially through get_role_oid_or_public
- Privilege checking is delegated to the central object_aclcheck function for consistency with other object types
- Function names in PostgreSQL can be overloaded, so convert_function_name must handle function signature resolution