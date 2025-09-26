# superuser_arg

## Location
src/backend/utils/misc/superuser.c: 56 - 102

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
  - `CacheRegisterSyscacheCallback`: Registers callback for cache invalidation
  - `RoleidCallback`: Callback function to invalidate cached results
  - `OidIsValid()`: Checks if OID is valid (implicit)
  - `IsUnderPostmaster`: Checks if running under postmaster (implicit)
  - `SearchSysCache1()`: Searches system catalog (implicit)
  - `HeapTupleIsValid()`: Validates heap tuple (implicit)
  - `GETSTRUCT()`: Extracts struct from heap tuple (implicit)
  - `ReleaseSysCache()`: Releases system cache entry (implicit)

- Called from (representative examples):
  - `superuser`: The parameterless wrapper function
  - `LockGXact`: Two-phase commit operations
  - `object_aclmask_ext`: Access control checks
  - `pg_class_aclmask_ext`: Table access permission checks
  - `CreateSubscription`: Logical replication setup
  - `check_role_membership_authorization`: Role membership verification
  - Various privilege and ownership checks throughout the system

## Notes and Other Information
- The function uses static variables `last_roleid`, `last_roleid_is_super`, and `roleid_callback_registered` for caching
- The bootstrap superuser (BOOTSTRAP_SUPERUSERID, typically OID 1) is treated specially to handle recovery scenarios
- Cache invalidation is handled through the `RoleidCallback` function, which is automatically called when pg_authid entries change
- Invalid role IDs are treated as non-superusers (return false)
- The callback registration happens only once per backend process
- Located in `src/backend/utils/misc/superuser.c:56-102`