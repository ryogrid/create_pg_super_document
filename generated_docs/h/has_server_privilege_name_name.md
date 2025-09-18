# has_server_privilege_name_name

## Location
src/backend/utils/adt/acl.c: 4007 - 4032

## Overview
Checks whether a named user has specified privileges on a named foreign server in PostgreSQL's foreign data wrapper system.

## Definition


## Detailed Description
This function is one of the SQL-callable variants of the has_server_privilege family. It takes three text/name arguments: username, server name, and privilege type, and returns a boolean indicating whether the specified user has the requested privilege on the foreign server. The function resolves the username and server name to their respective OIDs, converts the privilege string to an AclMode bitmask, and then performs the actual privilege check using the object access control system.

This function supports checking privileges for foreign servers, which are part of PostgreSQL's Foreign Data Wrapper (FDW) infrastructure that allows querying external data sources.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Arg 0:  - Name of the user whose privileges are being checked
  - Arg 1:  - Name of the foreign server
  - Arg 2:  - Comma-separated privilege names to check

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NAME
  - PG_GETARG_TEXT_PP
  - get_role_oid_or_public
  - [convert_server_name](../c/convert_server_name.md)
  - [convert_server_priv_string](../c/convert_server_priv_string.md)
  - [object_aclcheck](../o/object_aclcheck.md)
  - PG_RETURN_BOOL
  - Name (type)
  - AclResult (type)
  - AclMode (type)
  - ForeignServerRelationId
- Called from (representative examples):
  - SQL function calls to has_server_privilege(username, servername, privilege)

## Notes and Other Information
- This is a PostgreSQL SQL-callable function exposed to users for privilege checking
- Part of the Foreign Data Wrapper privilege checking infrastructure
- Returns true if the user has the privilege, false otherwise
- Handles both regular users and the special 'public' role through get_role_oid_or_public
- Uses the standard PostgreSQL access control framework via object_aclcheck
- Located in src/backend/utils/adt/acl.c:4007-4032