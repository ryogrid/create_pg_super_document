# RoleMembershipCacheCallback

## Location
[src/backend/utils/adt/acl.c:4937-4958](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L4937-L4958)

## Overview
This function serves as a system cache invalidation callback that clears role membership caches when relevant system catalog changes occur, ensuring cache consistency for role-based access control.

## Definition

```c
static void
RoleMembershipCacheCallback(Datum arg, int cacheid, uint32 hashvalue)
```
## Detailed Description
The `RoleMembershipCacheCallback` function is a static callback function registered with PostgreSQL's system cache invalidation mechanism. It is triggered whenever changes occur to the pg_auth_members, pg_authid, or pg_database system catalogs. The function invalidates cached role membership information by setting the cached_role array entries to InvalidOid, forcing the role membership caches to be recomputed on their next use. For database-related changes (DATABASEOID), it includes a optimization to ignore changes to other databases by comparing hash values, only processing changes relevant to the current database.

## Parameters / Member Variables
- `arg` (Datum): Callback argument (unused in this implementation)
- `cacheid` (int): Identifier specifying which system cache was invalidated
- `hashvalue` (uint32): Hash value of the invalidated cache entry

## Dependencies
- Functions called/Symbols referenced:
  - ROLERECURSE_MEMBERS
  - ROLERECURSE_PRIVS  
  - [ROLERECURSE_SETROLE](ROLERECURSE_SETROLE.md)
- Called from (representative examples):
  - [initialize_acl](../i/initialize_acl.md) (registered as callback for AUTHMEMROLEMEM, AUTHOID, and DATABASEOID)

## Notes and Other Information
- Function is static, indicating it's only used as a callback within the ACL subsystem
- Implements selective invalidation for database changes using cached_db_hash comparison
- Invalidates three types of role recursion caches: membership, privileges, and SET role capabilities
- Critical for maintaining cache consistency in multi-user environments where role memberships change
- Part of PostgreSQL's cache invalidation infrastructure ensuring ACL decisions reflect current system state
- Registered during initialize_acl() for three different system catalog types