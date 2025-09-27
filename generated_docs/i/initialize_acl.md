# initialize_acl

## Location
[src/backend/utils/adt/acl.c:4907-4936](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L4907-L4936)

## Overview
This function initializes the ACL (Access Control List) subsystem during PostgreSQL startup by setting up caching mechanisms and registering callback functions for role membership cache invalidation.

## Definition

```c
void
initialize_acl(void)
```
## Detailed Description
The `initialize_acl` function is called during PostgreSQL initialization (specifically by InitPostgres) to set up the ACL subsystem. It performs two main tasks: first, it caches the hash value of the current database OID for efficient access control checking, and second, it registers callback functions to handle cache invalidation when role membership or database information changes. The function only performs these operations when not in bootstrap processing mode, as the full catalog system is not available during bootstrap. The cache callbacks ensure that role membership information stays current when the underlying pg_auth_members, pg_authid, or pg_database system catalogs are modified.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - IsBootstrapProcessingMode
  - GetSysCacheHashValue1
  - [CacheRegisterSyscacheCallback](../C/CacheRegisterSyscacheCallback.md)
  - [RoleMembershipCacheCallback](../R/RoleMembershipCacheCallback.md)
- Called from (representative examples):
  - [InitPostgres](../I/InitPostgres.md)

## Notes and Other Information
- Called only once during PostgreSQL process initialization
- Skips initialization during bootstrap mode when catalog system is not fully available
- Sets up cache invalidation callbacks for three system catalog types: AUTHMEMROLEMEM, AUTHOID, and DATABASEOID
- The cached_db_hash variable stores the hash value of MyDatabaseId for performance optimization
- Critical for maintaining consistency of role-based access control throughout a PostgreSQL session
- Part of PostgreSQL's system initialization sequence ensuring ACL subsystem is properly configured

## Simplified Source

```c
// Simplified version of initialize_acl
void initialize_acl(void) {
    // Skip initialization during bootstrap mode
    if (!IsBootstrapProcessingMode()) {
        // Cache database hash for efficient access control checks
        cached_db_hash = GetSysCacheHashValue1(DATABASEOID,
                                               ObjectIdGetDatum(MyDatabaseId));

        // Register cache invalidation callbacks for role membership
        CacheRegisterSyscacheCallback(AUTHMEMROLEMEM,
                                      RoleMembershipCacheCallback,
                                      (Datum) 0);
        CacheRegisterSyscacheCallback(AUTHOID,
                                      RoleMembershipCacheCallback,
                                      (Datum) 0);
        CacheRegisterSyscacheCallback(DATABASEOID,
                                      RoleMembershipCacheCallback,
                                      (Datum) 0);
    }
}
```

Key simplifications made:
- Removed detailed comments and consolidated similar callback registrations
- Added explanatory comments for each main operation
- Preserved the bootstrap mode check and all three cache callback registrations
- Maintained the essential logic flow for ACL subsystem initialization