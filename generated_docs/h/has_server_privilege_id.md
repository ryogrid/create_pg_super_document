# has_server_privilege_id

## Location
[src/backend/utils/adt/acl.c:4087-4114](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L4087-L4114)

## Overview
Checks whether the current user has specified privileges on a foreign server identified by its OID, with support for missing object detection.

## Definition

```c
Datum
has_server_privilege_id(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is a variant of the has_server_privilege family that checks foreign server privileges for the current user using a server OID. It takes two arguments: the server OID and the privilege type, and automatically uses the current user's identity (obtained via GetUserId()). The function uses the extended access control check function (object_aclcheck_ext) which can detect when the target foreign server object is missing or has been dropped. If the server doesn't exist, the function returns NULL instead of raising an error.

This is a convenience function for checking the current user's privileges on a foreign server when the server OID is already known, providing both automatic user identification and graceful handling of missing objects.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Arg 0:  - OID of the foreign server
  - Arg 1:  - Comma-separated privilege names to check

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID
  - PG_GETARG_TEXT_PP
  - [GetUserId](../G/GetUserId.md)
  - [convert_server_priv_string](../c/convert_server_priv_string.md)
  - [object_aclcheck_ext](../o/object_aclcheck_ext.md)
  - PG_RETURN_NULL
  - PG_RETURN_BOOL
  - [AclResult](../A/AclResult.md) (type)
  - AclMode (type)
  - ForeignServerRelationId
- Called from (representative examples):
  - SQL function calls to has_server_privilege(serverid, privilege)

## Notes and Other Information
- This is a PostgreSQL SQL-callable function exposed to users for privilege checking
- Automatically uses the current user's identity, eliminating the need to specify a username
- Uses object_aclcheck_ext instead of object_aclcheck to handle missing objects gracefully
- Returns NULL if the foreign server object doesn't exist, rather than raising an error
- Part of the Foreign Data Wrapper privilege checking infrastructure
- Returns true if the current user has the privilege, false if not, NULL if server is missing
- Located in src/backend/utils/adt/acl.c:4087-4114