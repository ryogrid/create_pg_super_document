# has_server_privilege_name

## Location
[src/backend/utils/adt/acl.c:4033-4056](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L4033-L4056)

## Overview
Checks whether the current user has specified privileges on a named foreign server, using the currently authenticated user identity.

## Definition

```c
Datum
has_server_privilege_name(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is a variant of the has_server_privilege family that checks foreign server privileges for the current user. It takes two arguments: the server name and the privilege type, and automatically uses the current user's identity (obtained via GetUserId()). The function converts the server name to an OID, converts the privilege string to an AclMode bitmask, and then performs the actual privilege check using PostgreSQL's object access control system.

This is a convenience function for checking the current user's privileges without needing to explicitly specify the username, commonly used in scenarios where users want to check their own permissions on foreign servers.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Arg 0:  - Name of the foreign server
  - Arg 1:  - Comma-separated privilege names to check

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP
  - [GetUserId](../G/GetUserId.md)
  - [convert_server_name](../c/convert_server_name.md)
  - [convert_server_priv_string](../c/convert_server_priv_string.md)
  - [object_aclcheck](../o/object_aclcheck.md)
  - PG_RETURN_BOOL
  - [AclResult](../A/AclResult.md) (type)
  - AclMode (type)
  - ForeignServerRelationId
- Called from (representative examples):
  - SQL function calls to has_server_privilege(servername, privilege)

## Notes and Other Information
- This is a PostgreSQL SQL-callable function exposed to users for privilege checking
- Automatically uses the current user's identity, eliminating the need to specify a username
- Part of the Foreign Data Wrapper privilege checking infrastructure
- Returns true if the current user has the privilege, false otherwise
- Uses the standard PostgreSQL access control framework via object_aclcheck
- Located in src/backend/utils/adt/acl.c:4033-4056

## Simplified Source

```c
Datum
has_server_privilege_name(PG_FUNCTION_ARGS)
{
    text      *servername = PG_GETARG_TEXT_PP(0);
    text      *priv_type_text = PG_GETARG_TEXT_PP(1);
    Oid        roleid;
    Oid        serverid;
    AclMode    mode;
    AclResult  aclresult;

    // Use current user ID
    roleid = GetUserId();

    // Convert server name to server OID
    serverid = convert_server_name(servername);

    // Convert privilege string to access mode
    mode = convert_server_priv_string(priv_type_text);

    // Check if current user has the specified privilege on the foreign server
    aclresult = object_aclcheck(ForeignServerRelationId, serverid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}
```