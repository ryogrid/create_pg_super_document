# pg_has_role_name_id

## Location
[src/backend/utils/adt/acl.c:4755-4778](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L4755-L4778)

## Overview
Checks if a user (specified by name) has specific privileges on a role (specified by OID).

## Definition


## Detailed Description
This function is a mixed-parameter variant of the pg_has_role privilege checking system that accepts a user name and a role OID. It combines name-based user identification with OID-based role identification, which can be useful when the role OID is already known but the username needs to be resolved. The function converts the username to its corresponding OID and then performs the standard role privilege check.

## Parameters / Member Variables
-  (Name username): The name of the user whose privileges are being checked
-  (Oid roleoid): The OID of the role on which privileges are being checked
-  (text priv_type_text): The privilege type as a text string (e.g., 'USAGE', 'MEMBER')

## Dependencies
- Functions called/Symbols referenced:
  - : Converts username to user OID
  - : Converts privilege string to AclMode
  - : Performs the actual privilege check
  - : PostgreSQL macro to extract Name arguments
  - : PostgreSQL macro to extract OID arguments
  - : PostgreSQL macro to extract text arguments
  - : PostgreSQL macro to return boolean result
- Called from (representative examples):
  - SQL queries using pg_has_role(username, role_oid, privilege) function

## Notes and Other Information
- This function provides a hybrid approach where the user is specified by name but the role by OID
- Returns true if the specified user has the specified privilege on the role, false otherwise
- Located in src/backend/utils/adt/acl.c:4755-4778
- Part of the comprehensive pg_has_role function family that covers all combinations of name/OID parameters
- Useful when role OIDs are available from system catalogs but usernames are more convenient for specification