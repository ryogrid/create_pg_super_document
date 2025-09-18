# has_parameter_privilege_name

## Location
src/backend/utils/adt/acl.c: 4644 - 4657

## Overview
A PostgreSQL function that checks if the current user has specific privileges on a parameter, taking the parameter name and privilege name as text arguments.

## Definition


## Detailed Description
This function is a simplified variant of the has_parameter_privilege family that assumes the current user as the subject of the privilege check. It takes only the parameter name and privilege name as arguments, automatically using the current session's user ID for the privilege check. This provides a convenient interface for users to check their own privileges on parameters without having to specify their username explicitly.

The function converts the privilege string to an AclMode and delegates the actual checking to the helper function  using the current user's ID obtained via .

## Parameters / Member Variables
- Function uses  macro to access PostgreSQL function arguments:
  - Arg 0:  (text) - The name of the parameter
  - Arg 1:  (text) - The privilege name as a string (e.g., 'SET', 'ALTER SYSTEM')

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP (extracts text arguments)
  - convert_parameter_priv_string (converts privilege string to AclMode)
  - GetUserId (retrieves current user's OID)
  - has_param_priv_byname (performs the actual privilege check)
- Called from (representative examples):
  - SQL queries using has_parameter_privilege(parameter, privilege) syntax

## Notes and Other Information
- Returns a Datum containing a boolean result (true if current user has privilege, false otherwise)
- Uses PostgreSQL's function calling convention with PG_FUNCTION_ARGS
- Automatically assumes current_user, making it convenient for self-privilege checks
- Part of the SQL-accessible interface for parameter privilege checking
- The function name 'name' indicates it takes parameter and privilege names as arguments
- This is the two-argument version of has_parameter_privilege, contrasting with the three-argument version that explicitly specifies the username