# has_database_privilege_name_id

## Location
src/backend/utils/adt/acl.c: 3040 - 3069

## Overview
Checks if a specified user has a given privilege on a database identified by OID, where the user is specified by name and the privilege by string.

## Definition


## Detailed Description
This PostgreSQL function implements a hybrid variant of the has_database_privilege SQL function family. It takes a username (by name), a database OID (by ID), and a privilege type string, then determines whether the specified user has the requested privilege on the database. This variant is optimized for cases where the database OID is already known but the username needs to be resolved. The function uses object_aclcheck_ext to handle cases where the database might not exist, returning NULL in such situations.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to:
  -  (Name): The name of the user whose privileges are being checked
  -  (Oid): The OID of the database to check privileges for
  -  (text): The privilege type as a string (e.g., "CONNECT", "CREATE", "TEMPORARY")

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NAME
  - PG_GETARG_OID
  - PG_GETARG_TEXT_PP
  - get_role_oid_or_public
  - convert_database_priv_string
  - object_aclcheck_ext
  - PG_RETURN_NULL
  - PG_RETURN_BOOL
- Called from (representative examples):
  - No direct references found (likely called via SQL function dispatch)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NAME
  - PG_GETARG_OID 
  - PG_GETARG_TEXT_PP
  - get_role_oid_or_public
  - convert_database_priv_string
  - object_aclcheck_ext
  - PG_RETURN_NULL
  - PG_RETURN_BOOL
- Called from (representative examples):
  - No direct references found (likely called via SQL function dispatch)

## Notes and Other Information
- This function is part of the has_database_privilege family of SQL functions, all named "has_database_privilege" at the SQL level
- Returns a boolean: true if the user has the privilege, false if not, or NULL if the database doesn't exist
- Uses object_aclcheck_ext instead of object_aclcheck to properly handle missing database objects
- Hybrid approach: resolves username to OID but uses database OID directly, making it efficient when database OID is known
- The is_missing flag from object_aclcheck_ext allows proper NULL return when the database doesn't exist
- Located in src/backend/utils/adt/acl.c:3040-3069