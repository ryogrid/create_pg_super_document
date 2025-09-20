# pg_has_role_name_name

## Location
[src/backend/utils/adt/acl.c:4705-4730](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L4705-L4730)

## Overview
Checks if a user has specific privileges on a role, where both the user and role are specified by their names.

## Definition

```c
Datum
pg_has_role_name_name(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is part of the PostgreSQL privilege checking system for roles. It takes three parameters via the PostgreSQL function call interface: a username, a rolename, and a privilege type string. The function resolves the names to their corresponding OIDs and then delegates the actual privilege checking to the core ACL (Access Control List) checking mechanism. This function is exposed at the SQL level as one of the variants of the  function.

## Parameters / Member Variables
-  (Name username): The name of the user whose privileges are being checked
-  (Name rolename): The name of the role on which privileges are being checked  
-  (text priv_type_text): The privilege type as a text string (e.g., 'USAGE', 'MEMBER')

## Dependencies
- Functions called/Symbols referenced:
  - : Converts role name to OID
  - : Converts privilege string to AclMode
  - : Performs the actual privilege check
  - : PostgreSQL macro to extract Name arguments
  - : PostgreSQL macro to extract text arguments
  - : PostgreSQL macro to return boolean result
- Called from (representative examples):
  - SQL queries using pg_has_role(username, rolename, privilege) function

## Notes and Other Information
- This is one of several variants of the pg_has_role function family that handle different combinations of role name/OID and user name/OID
- Returns true if the user has the specified privilege on the role, false otherwise
- Located in src/backend/utils/adt/acl.c:4705-4730
- The function follows PostgreSQL's standard function call interface using PG_FUNCTION_ARGS