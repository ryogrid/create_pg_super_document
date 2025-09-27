# superuser_arg

## Location
[src/backend/utils/misc/superuser.c:56-102](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/superuser.c#L56-L102)

## Overview
Determines whether a specified role (identified by OID) has PostgreSQL superuser privileges, with caching for performance optimization.

## Definition
```c
bool superuser_arg(Oid roleid)
```

## Detailed Description
The `superuser_arg` function is the core implementation for checking superuser privileges in PostgreSQL. It takes a role OID as input and returns true if that role has superuser privileges. The function implements several optimization and fallback mechanisms:

1. **Caching**: Uses static variables to cache the last queried role and result to avoid repeated system catalog lookups
2. **Bootstrap mode handling**: Provides special handling for the bootstrap superuser (OID 1) when not running under postmaster
3. **System catalog lookup**: Queries the `pg_authid` system catalog to check the `rolsuper` field
4. **Cache invalidation**: Registers a syscache callback to invalidate the cache when role information changes

The function is designed to be efficient for repeated calls with the same role ID, which is common in PostgreSQL operations.

## Parameters / Member Variables
- `roleid`: The OID of the role to check for superuser privileges

## Dependencies
- Functions called/Symbols referenced:
  - `Form_pg_authid`: Structure representing pg_authid catalog entries
  - `[CacheRegisterSyscacheCallback](../C/CacheRegisterSyscacheCallback.md)`: Registers callback for cache invalidation
  - `[RoleidCallback](../R/RoleidCallback.md)`: Callback function to invalidate cached results
  - `OidIsValid()`: Checks if OID is valid (implicit)
  - `IsUnderPostmaster`: Checks if running under postmaster (implicit)
  - `[SearchSysCache1](../S/SearchSysCache1.md)()`: Searches system catalog (implicit)
  - `HeapTupleIsValid()`: Validates heap tuple (implicit)
  - `GETSTRUCT()`: Extracts struct from heap tuple (implicit)
  - `[ReleaseSysCache](../R/ReleaseSysCache.md)()`: Releases system cache entry (implicit)

- Called from (representative examples):
  - `[superuser](superuser.md)`: The parameterless wrapper function
  - `[LockGXact](../L/LockGXact.md)`: Two-phase commit operations
  - `[object_aclmask_ext](../o/object_aclmask_ext.md)`: Access control checks
  - `[pg_class_aclmask_ext](../p/pg_class_aclmask_ext.md)`: Table access permission checks
  - `[CreateSubscription](../C/CreateSubscription.md)`: Logical replication setup
  - `[check_role_membership_authorization](../c/check_role_membership_authorization.md)`: Role membership verification
  - Various privilege and ownership checks throughout the system

## Notes and Other Information
- The function uses static variables `last_roleid`, `last_roleid_is_super`, and `roleid_callback_registered` for caching
- The bootstrap superuser (BOOTSTRAP_SUPERUSERID, typically OID 1) is treated specially to handle recovery scenarios
- Cache invalidation is handled through the `RoleidCallback` function, which is automatically called when pg_authid entries change
- Invalid role IDs are treated as non-superusers (return false)
- The callback registration happens only once per backend process
- Located in `src/backend/utils/misc/superuser.c:56-102`

## Simplified Source

```c
// Simplified version of superuser_arg
bool superuser_arg(Oid roleid) {
    bool result;
    HeapTuple rtup;

    // Quick cache check - return cached result if available
    if (OidIsValid(last_roleid) && last_roleid == roleid)
        return last_roleid_is_super;

    // Special case: bootstrap superuser when not under postmaster
    if (!IsUnderPostmaster && roleid == BOOTSTRAP_SUPERUSERID)
        return true;

    // Look up the role in pg_authid system catalog
    rtup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));
    if (HeapTupleIsValid(rtup)) {
        // Extract the rolsuper field from the tuple
        result = ((Form_pg_authid) GETSTRUCT(rtup))->rolsuper;
        ReleaseSysCache(rtup);
    } else {
        // Invalid role ID - treat as non-superuser
        result = false;
    }

    // Set up cache invalidation callback on first use
    if (!roleid_callback_registered) {
        CacheRegisterSyscacheCallback(AUTHOID, RoleidCallback, (Datum) 0);
        roleid_callback_registered = true;
    }

    // Cache the result for future queries
    last_roleid = roleid;
    last_roleid_is_super = result;

    return result;
}
```

Key simplifications made:
- Added clear comments explaining each major step
- Grouped related logic sections together (cache check, bootstrap case, catalog lookup, etc.)
- Explained the purpose of cache invalidation callback registration
- Clarified the bootstrap superuser special case
- Maintained all performance optimizations and safety checks
- Preserved the efficient caching mechanism