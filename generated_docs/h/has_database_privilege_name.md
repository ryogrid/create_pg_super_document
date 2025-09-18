# has_database_privilege_name

## Location
src/backend/utils/adt/acl.c: 3016 - 3039

## Overview
Checks if the current user has a specified privilege on a given database, using the database name and privilege type as parameters.

## Definition


## Detailed Description
This PostgreSQL function implements a variant of the has_database_privilege SQL function family that assumes the current user as the subject of the privilege check. It takes two text arguments: a database name and a privilege type string, then determines whether the current user has the requested privilege on the specified database. The function uses GetUserId() to identify the current user, converts the database name to its OID, converts the privilege string to an AclMode bitmask, and performs the access control check.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to:
  -  (text): The name of the database to check privileges for
  -  (text): The privilege type as a string (e.g., "CONNECT", "CREATE", "TEMPORARY")

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP
  - [GetUserId](../G/GetUserId.md)
  - [convert_database_name](../c/convert_database_name.md)
  - [convert_database_priv_string](../c/convert_database_priv_string.md)
  - [object_aclcheck](../o/object_aclcheck.md)
  - PG_RETURN_BOOL
- Called from (representative examples):
  - No direct references found (likely called via SQL function dispatch)

## Notes and Other Information
- This function is part of the has_database_privilege family of SQL functions, all named "has_database_privilege" at the SQL level
- Returns a boolean: true if the current user has the privilege, false if not, or NULL if the database doesn't exist
- More convenient variant when checking privileges for the current user, avoiding the need to specify a username
- Uses GetUserId() to automatically determine the current user's OID rather than requiring it as a parameter
- The actual privilege checking is delegated to the generic object_aclcheck function with DatabaseRelationId
- Located in src/backend/utils/adt/acl.c:3016-3039