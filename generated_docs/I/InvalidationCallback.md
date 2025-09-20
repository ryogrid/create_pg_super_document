# InvalidationCallback

## Location
[src/backend/catalog/namespace.c:4796-4818](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L4796-L4818)

## Overview
A syscache invalidation callback function that invalidates the search path cache when relevant system catalog changes occur.

## Definition

```c
static void
InvalidationCallback(Datum arg, int cacheid, uint32 hashvalue)
```
## Detailed Description
This static function serves as a syscache invalidation callback that is triggered whenever changes occur to system catalogs that affect search path resolution. When called, it invalidates both the base search path and the search path cache, forcing them to be recomputed on the next access. This ensures that search path resolution remains consistent with any changes to namespace names, access control lists (ACLs), role names, or role memberships that could affect which schemas are accessible or visible to the current user.

## Parameters / Member Variables
- : A Datum argument passed when the callback was registered (currently unused)
- : The identifier of the system cache that was invalidated
- : The hash value of the invalidated cache entry

## Dependencies
- Functions called/Symbols referenced:
  - None (only modifies global variables)
- Called from (representative examples):
  - [InitializeSearchPath](InitializeSearchPath.md) (registered for NAMESPACEOID, AUTHOID, AUTHMEMROLEMEM, DATABASEOID caches)

## Notes and Other Information
- Declared as static, so it's only accessible within namespace.c
- Registered during InitializeSearchPath for multiple syscaches that can affect search paths
- Uses a simple but effective strategy: invalidate everything and recompute on demand
- Critical for maintaining search path consistency in a multi-user environment
- The function parameters follow the standard syscache callback signature but only the function call itself matters for invalidation
- Both baseSearchPathValid and searchPathCacheValid are set to false to ensure complete cache invalidation