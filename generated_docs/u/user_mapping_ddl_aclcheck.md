# user_mapping_ddl_aclcheck

## Location
[src/backend/commands/foreigncmds.c:1086-1110](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/foreigncmds.c#L1086-L1110)

## Overview
A common utility function that performs access control checks for user-mapping-related DDL commands, ensuring that only server owners or users operating on their own mappings can perform the requested operations.

## Definition

```c
static void
user_mapping_ddl_aclcheck(Oid umuserid, Oid serverid, const char *servername)
```
## Detailed Description
This static function implements a centralized permission checking mechanism for user mapping DDL operations. It enforces a two-tier access control policy: server owners have full privileges to operate on any user mapping associated with their server, while regular users can only operate on their own user mappings. The function first checks if the current user owns the foreign server; if not, it verifies whether the user is attempting to operate on their own mapping and has USAGE privileges on the server.

## Parameters / Member Variables
- `umuserid`: The OID of the user whose mapping is being operated on
- `serverid`: The OID of the foreign server associated with the user mapping
- `*servername`: The name of the foreign server (used for error reporting)
## Dependencies
- Functions called/Symbols referenced:
  - [GetUserId](../G/GetUserId.md)
  - [object_ownercheck](../o/object_ownercheck.md)
  - [object_aclcheck](../o/object_aclcheck.md)
  - [aclcheck_error](../a/aclcheck_error.md)
  - ACL_USAGE
  - ACLCHECK_NOT_OWNER
  - OBJECT_FOREIGN_SERVER
- Called from (representative examples):
  - [CreateUserMapping](../C/CreateUserMapping.md)
  - [AlterUserMapping](../A/AlterUserMapping.md)
  - [RemoveUserMapping](../R/RemoveUserMapping.md)

## Notes and Other Information
This function serves as a security gate for all user mapping DDL operations, centralizing the access control logic to ensure consistency across different commands. The function uses PostgreSQL's standard ACL (Access Control List) checking mechanisms and follows the principle of least privilege by allowing users to modify only their own mappings unless they own the entire server.

## Simplified Source

```c
static void
user_mapping_ddl_aclcheck(Oid umuserid, Oid serverid, const char *servername)
{
    Oid curuserid = GetUserId();

    // Check if current user owns the foreign server
    if (!object_ownercheck(ForeignServerRelationId, serverid, curuserid))
    {
        // Non-owner can only operate on their own mapping
        if (umuserid == curuserid)
        {
            // Check if user has USAGE privilege on the server
            AclResult aclresult = object_aclcheck(ForeignServerRelationId, serverid,
                                                curuserid, ACL_USAGE);
            if (aclresult != ACLCHECK_OK)
                aclcheck_error(aclresult, OBJECT_FOREIGN_SERVER, servername);
        }
        else
        {
            // Cannot operate on other users' mappings
            aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_FOREIGN_SERVER, servername);
        }
    }
    // Server owners can operate on any mapping - no additional checks needed
}
```