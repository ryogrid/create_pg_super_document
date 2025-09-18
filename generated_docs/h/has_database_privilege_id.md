# has_database_privilege_id

## Location
src/backend/utils/adt/acl.c: 3070 - 3097

## Overview
Checks if the current user has a specified privilege on a database identified by OID, providing the most efficient variant when both user and database are known by ID.

## Definition


## Detailed Description
This PostgreSQL function implements the most efficient variant of the has_database_privilege SQL function family. It takes a database OID and a privilege type string, then determines whether the current user has the requested privilege on the specified database. By using the current user (via GetUserId()) and the database OID directly, this variant avoids all name-to-OID conversions, making it the fastest option. The function uses object_aclcheck_ext to properly handle cases where the database might not exist.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to:
  -  (Oid): The OID of the database to check privileges for
  -  (text): The privilege type as a string (e.g., "CONNECT", "CREATE", "TEMPORARY")

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID
  - PG_GETARG_TEXT_PP
  - GetUserId
  - convert_database_priv_string
  - object_aclcheck_ext
  - PG_RETURN_NULL
  - PG_RETURN_BOOL
- Called from (representative examples):
  - No direct references found (likely called via SQL function dispatch)

## Notes and Other Information
- This function is part of the has_database_privilege family of SQL functions, all named "has_database_privilege" at the SQL level
- Returns a boolean: true if the current user has the privilege, false if not, or NULL if the database doesn't exist
- Most efficient variant as it avoids any name-to-OID conversions for both user and database
- Uses GetUserId() to automatically determine the current user's OID
- Uses object_aclcheck_ext instead of object_aclcheck to properly handle missing database objects
- The is_missing flag allows proper NULL return when the database doesn't exist
- Ideal for internal PostgreSQL code that already has database OIDs available
- Located in src/backend/utils/adt/acl.c:3070-3097