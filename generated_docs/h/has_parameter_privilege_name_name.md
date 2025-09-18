# has_parameter_privilege_name_name

## Location
src/backend/utils/adt/acl.c: 4628 - 4643

## Overview
A PostgreSQL function that checks if a named user has specific privileges on a parameter, taking all arguments as text values.

## Definition


## Detailed Description
This function is one of the SQL-accessible variants of the has_parameter_privilege family. It accepts a username (as a Name type), parameter name (as text), and privilege name (as text), then performs the privilege check. The function converts the username to a role OID and the privilege string to an AclMode, then delegates the actual checking to the helper function .

This function is designed to be called directly from SQL queries where users want to check parameter privileges using human-readable names for both the user and the privilege type.

## Parameters / Member Variables
- Function uses  macro to access PostgreSQL function arguments:
  - Arg 0:  (Name) - The name of the user/role whose privileges are being checked
  - Arg 1:  (text) - The name of the parameter
  - Arg 2:  (text) - The privilege name as a string (e.g., 'SET', 'ALTER SYSTEM')

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NAME (extracts Name argument)
  - PG_GETARG_TEXT_PP (extracts text arguments)
  - [convert_parameter_priv_string](../c/convert_parameter_priv_string.md) (converts privilege string to AclMode)
  - get_role_oid_or_public (converts username to role OID)
  - [has_param_priv_byname](has_param_priv_byname.md) (performs the actual privilege check)
- Called from (representative examples):
  - SQL queries using has_parameter_privilege(username, parameter, privilege) syntax

## Notes and Other Information
- Returns a Datum containing a boolean result (true if user has privilege, false otherwise)
- Uses PostgreSQL's function calling convention with PG_FUNCTION_ARGS
- Part of the SQL-accessible interface for parameter privilege checking
- Handles the conversion of text-based privilege names to internal representations
- The function name follows PostgreSQL's naming convention where 'name_name' indicates the first argument is a username (Name type) and other arguments are names/text