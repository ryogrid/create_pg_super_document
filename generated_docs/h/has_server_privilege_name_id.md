# has_server_privilege_name_id

## Location
[src/backend/utils/adt/acl.c:4057-4086](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L4057-L4086)

## Overview
Checks whether a named user has specified privileges on a foreign server identified by its OID, with support for missing object detection.

## Definition

```c
Datum
has_server_privilege_name_id(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is a variant of the has_server_privilege family that takes a username, server OID, and privilege type as arguments. It uses the extended access control check function (object_aclcheck_ext) which can detect when the target foreign server object is missing or has been dropped. If the server doesn't exist, the function returns NULL instead of raising an error, providing a more graceful handling of missing objects.

This function is useful when working with foreign server OIDs directly, which might happen in administrative queries or when the server OID is already known from other system operations.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Arg 0:  - Name of the user whose privileges are being checked
  - Arg 1:  - OID of the foreign server
  - Arg 2:  - Comma-separated privilege names to check

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NAME
  - PG_GETARG_OID
  - PG_GETARG_TEXT_PP
  - [get_role_oid_or_public](../g/get_role_oid_or_public.md)
  - [convert_server_priv_string](../c/convert_server_priv_string.md)
  - [object_aclcheck_ext](../o/object_aclcheck_ext.md)
  - PG_RETURN_NULL
  - PG_RETURN_BOOL
  - Name (type)
  - [AclResult](../A/AclResult.md) (type)
  - AclMode (type)
  - ForeignServerRelationId
- Called from (representative examples):
  - SQL function calls to has_server_privilege(username, serverid, privilege)

## Notes and Other Information
- This is a PostgreSQL SQL-callable function exposed to users for privilege checking
- Uses object_aclcheck_ext instead of object_aclcheck to handle missing objects gracefully
- Returns NULL if the foreign server object doesn't exist, rather than raising an error
- Part of the Foreign Data Wrapper privilege checking infrastructure
- Returns true if the user has the privilege, false if not, NULL if server is missing
- Handles both regular users and the special 'public' role through get_role_oid_or_public
- Located in src/backend/utils/adt/acl.c:4057-4086