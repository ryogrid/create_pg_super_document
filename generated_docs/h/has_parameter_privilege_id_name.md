# has_parameter_privilege_id_name

## Location
[src/backend/utils/adt/acl.c:4658-4675](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L4658-L4675)

## Overview
A PostgreSQL function that checks if a user (identified by role OID) has specific privileges on a parameter, with the parameter name and privilege name as text arguments.

## Definition

```c
Datum
has_parameter_privilege_id_name(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is another variant of the has_parameter_privilege family that accepts a role OID directly rather than requiring username resolution. It takes a role OID, parameter name, and privilege name as arguments, then performs the privilege check. This variant is useful when the caller already has the role OID available, avoiding the overhead of username-to-OID conversion.

The function converts the privilege string to an AclMode and delegates the actual checking to the helper function  using the provided role OID directly.

## Parameters / Member Variables
- Function uses  macro to access PostgreSQL function arguments:
  - Arg 0:  (Oid) - The OID of the user/role whose privileges are being checked
  - Arg 1:  (text) - The name of the parameter
  - Arg 2:  (text) - The privilege name as a string (e.g., 'SET', 'ALTER SYSTEM')

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID (extracts OID argument)
  - PG_GETARG_TEXT_PP (extracts text arguments)
  - [convert_parameter_priv_string](../c/convert_parameter_priv_string.md) (converts privilege string to AclMode)
  - [has_param_priv_byname](has_param_priv_byname.md) (performs the actual privilege check)
- Called from (representative examples):
  - SQL queries using has_parameter_privilege(role_oid, parameter, privilege) syntax

## Notes and Other Information
- Returns a Datum containing a boolean result (true if user has privilege, false otherwise)
- Uses PostgreSQL's function calling convention with PG_FUNCTION_ARGS
- More efficient than name-based variants when the role OID is already known
- Part of the SQL-accessible interface for parameter privilege checking
- The function name 'id_name' indicates the first argument is an ID (OID) and others are names/text
- Avoids the username resolution step that would be required in the name_name variant