# pg_has_role_id

## Location
[src/backend/utils/adt/acl.c:4779-4800](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L4779-L4800)

## Overview
Checks if the current user has specific privileges on a role specified by OID, using the current user as the default subject.

## Definition


## Detailed Description
This function is an OID-based variant of the pg_has_role privilege checking system that assumes the current user as the subject of the privilege check. It takes only the target role OID and privilege type as parameters, automatically using the current session's user ID for the privilege verification. This function is optimized for cases where the role OID is already known and no name resolution is needed, making it more efficient than name-based variants.

## Parameters / Member Variables
-  (Oid roleoid): The OID of the role on which privileges are being checked
-  (text priv_type_text): The privilege type as a text string (e.g., 'USAGE', 'MEMBER')

## Dependencies
- Functions called/Symbols referenced:
  - : Gets the OID of the current user session
  - : Converts privilege string to AclMode
  - : Performs the actual privilege check
  - : PostgreSQL macro to extract OID arguments
  - : PostgreSQL macro to extract text arguments
  - : PostgreSQL macro to return boolean result
- Called from (representative examples):
  - SQL queries using pg_has_role(role_oid, privilege) function

## Notes and Other Information
- This function automatically uses the current user session (via GetUserId()) as the subject for privilege checking
- Most efficient variant of pg_has_role when role OID is already available, as it avoids name-to-OID resolution
- Returns true if the current user has the specified privilege on the role, false otherwise
- Located in src/backend/utils/adt/acl.c:4779-4800
- Commonly used in internal PostgreSQL code where role OIDs are readily available from system catalogs
- Part of the pg_has_role function family that provides different parameter combinations for role privilege checking