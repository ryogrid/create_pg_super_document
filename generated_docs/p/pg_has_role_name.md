# pg_has_role_name

## Location
[src/backend/utils/adt/acl.c:4731-4754](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L4731-L4754)

## Overview
Checks if the current user has specific privileges on a role specified by name, using the current user as the default subject.

## Definition

```c
Datum
pg_has_role_name(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is a variant of the pg_has_role privilege checking system that assumes the current user as the subject of the privilege check. It takes only the target role name and privilege type as parameters, automatically using the current session's user ID for the privilege verification. This is a convenience function that eliminates the need to explicitly specify the current user when checking role privileges.

## Parameters / Member Variables
-  (Name rolename): The name of the role on which privileges are being checked
-  (text priv_type_text): The privilege type as a text string (e.g., 'USAGE', 'MEMBER')

## Dependencies
- Functions called/Symbols referenced:
  - : Gets the OID of the current user session
  - : Converts role name to OID
  - : Converts privilege string to AclMode
  - : Performs the actual privilege check
  - : PostgreSQL macro to extract Name arguments
  - : PostgreSQL macro to extract text arguments
  - : PostgreSQL macro to return boolean result
- Called from (representative examples):
  - SQL queries using pg_has_role(rolename, privilege) function

## Notes and Other Information
- This function automatically uses the current user session (via GetUserId()) as the subject for privilege checking
- Returns true if the current user has the specified privilege on the role, false otherwise
- Located in src/backend/utils/adt/acl.c:4731-4754
- Part of the pg_has_role function family that provides different parameter combinations for role privilege checking
- Commonly used in SQL queries where the current user's privileges need to be verified against a specific role