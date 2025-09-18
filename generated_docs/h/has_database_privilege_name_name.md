# has_database_privilege_name_name

## Location
src/backend/utils/adt/acl.c: 2990 - 3015

## Overview
Checks if a specified user has a given privilege on a specified database, where all parameters are provided as names (not OIDs).

## Definition


## Detailed Description
This PostgreSQL function implements one variant of the has_database_privilege SQL function family. It takes three text arguments: a username, a database name, and a privilege type string, then determines whether the specified user has the requested privilege on the specified database. The function converts the name-based parameters to their corresponding OIDs, converts the privilege string to an AclMode bitmask, and then performs the actual access control check using the object_aclcheck function.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to:
  -  (Name): The name of the user whose privileges are being checked
  -  (text): The name of the database to check privileges for
  -  (text): The privilege type as a string (e.g., "CONNECT", "CREATE", "TEMPORARY")

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NAME
  - PG_GETARG_TEXT_PP
  - get_role_oid_or_public
  - convert_database_name
  - convert_database_priv_string
  - object_aclcheck
  - PG_RETURN_BOOL
- Called from (representative examples):
  - No direct references found (likely called via SQL function dispatch)

## Notes and Other Information
- This function is part of the has_database_privilege family of SQL functions, all named "has_database_privilege" at the SQL level
- Returns a boolean: true if the user has the privilege, false if not, or NULL if the database doesn't exist
- Uses name-to-OID conversion for both username and database name, making it user-friendly but potentially slower than OID-based variants
- The actual privilege checking is delegated to the generic object_aclcheck function with DatabaseRelationId
- Located in src/backend/utils/adt/acl.c:2990-3015