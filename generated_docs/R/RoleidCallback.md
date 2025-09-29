# RoleidCallback

## Location
[src/backend/utils/misc/superuser.c:103-107](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/superuser.c#L103-L107)

## Overview
A syscache invalidation callback function that clears the cached superuser status when role information changes in the pg_authid catalog.

## Definition
```c
static void RoleidCallback(Datum arg, int cacheid, uint32 hashvalue)
```

## Detailed Description
The `RoleidCallback` function is a syscache invalidation callback that is registered to be called whenever there are changes to the `pg_authid` system catalog. Its primary purpose is to maintain cache coherency for the superuser privilege checking mechanism implemented in `superuser_arg`.

When PostgreSQL's system catalog cache detects that entries in pg_authid have been modified (such as when a role's superuser status is changed via ALTER ROLE), this callback is automatically invoked. The function responds by invalidating the local cache maintained by the superuser checking functions, ensuring that subsequent superuser checks will query the updated catalog data rather than using stale cached information.

The function is declared as `static` since it is only used internally within the superuser.c module and is registered as a callback rather than being called directly by other code.

## Parameters / Member Variables
- `arg`: A Datum argument passed to the callback (unused in this implementation)
- `cacheid`: The cache ID that triggered the invalidation (typically AUTHOID for pg_authid)  
- `hashvalue`: The hash value associated with the invalidated cache entry (unused in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - `InvalidOid`: Constant used to mark the cached role ID as invalid
  - Uses static variable `last_roleid` from the same module

- Called from (representative examples):
  - `[superuser_arg](../s/superuser_arg.md)`: Registers this callback via `CacheRegisterSyscacheCallback`
  - PostgreSQL syscache system: Automatically invoked when pg_authid changes occur

## Notes and Other Information
- This is a classic example of a cache invalidation callback in PostgreSQL's syscache system
- The callback takes a simple but effective approach: it invalidates all cached superuser status by setting `last_roleid` to `InvalidOid`
- This invalidation strategy means that the next call to `superuser_arg` will perform a fresh lookup from pg_authid
- The callback is registered only once per backend process, the first time `superuser_arg` is called
- The function parameters follow the standard signature for PostgreSQL syscache callbacks
- Located in `src/backend/utils/misc/superuser.c:103-107`

## Simplified Source

```c
// Simplified version of RoleidCallback
static void RoleidCallback(Datum arg, int cacheid, uint32 hashvalue) {
    // Core logic: Invalidate cached superuser status when role data changes
    last_roleid = InvalidOid;
}
```

Key simplifications made:
- Preserved the essential cache invalidation logic
- Removed unused parameters (arg, cacheid, hashvalue are not used in the implementation)
- Maintained the static function signature required for syscache callbacks
- Kept the single critical operation: setting last_roleid to InvalidOid
- Added explanatory comment describing the core purpose