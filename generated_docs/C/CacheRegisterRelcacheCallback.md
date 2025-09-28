# CacheRegisterRelcacheCallback

## Location
[src/backend/utils/cache/inval.c:1561-1579](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/inval.c#L1561-L1579)

## Overview
Registers a callback function to be invoked for all future relation cache invalidation events.

## Definition
```c
void CacheRegisterRelcacheCallback(RelcacheCallbackFunction func, Datum arg)
```

## Detailed Description
CacheRegisterRelcacheCallback allows subsystems to register callback functions that will be automatically invoked whenever relation cache invalidation events occur. This is essential for maintaining secondary data structures or caches that depend on relation metadata and need to be updated when relations are modified.

Key characteristics:
- **Global relation monitoring**: Unlike syscache callbacks which are cache-specific, relcache callbacks monitor all relation invalidations
- **OID-based notification**: The callback receives the OID of the invalidated relation
- **Reset handling**: InvalidOid indicates a cache reset request, requiring callbacks to flush all cached relation state
- **Simple registration**: Uses a simpler registration mechanism than syscache callbacks (no chaining, just sequential array)
- **Limited capacity**: The system supports up to MAX_RELCACHE_CALLBACKS total callbacks

The function maintains a global callback list and simply appends new callbacks to the end of the array. All registered callbacks are invoked in registration order when any relation cache invalidation occurs.

## Parameters / Member Variables
- `func`: RelcacheCallbackFunction pointer to the callback function to invoke on relation invalidation events
- `arg`: Datum argument to pass to the callback function when it's invoked

## Dependencies
- Functions called/Symbols referenced:
  - MAX_RELCACHE_CALLBACKS (maximum callback limit constant)
  - relcache_callback_count (global counter for registered callbacks)
  - relcache_callback_list (global array storing callback information)
  - elog (error logging function)
- Called from (representative examples):
  - [logicalrep_relmap_init](../l/logicalrep_relmap_init.md) (src/backend/replication/logical/relation.c:124)
  - [logicalrep_partmap_init](../l/logicalrep_partmap_init.md) (src/backend/replication/logical/relation.c:586)
  - [init_rel_sync_cache](../i/init_rel_sync_cache.md) (src/backend/replication/pgoutput/pgoutput.c:1942)
  - [InitPlanCache](../I/InitPlanCache.md) (src/backend/utils/cache/plancache.c:157)
  - [InitializeRelfilenumberMap](../I/InitializeRelfilenumberMap.md) (src/backend/utils/cache/relfilenumbermap.c:125)
  - [lookup_type_cache](../l/lookup_type_cache.md) (src/backend/utils/cache/typcache.c:362)

## Notes and Other Information
- The callback registration is permanent for the lifetime of the backend process
- All callbacks are invoked for every relation invalidation event, regardless of which specific relation was invalidated
- InvalidOid has special meaning (cache reset) and requires callbacks to flush all relation-related cached state
- Simpler than syscache callbacks since there's no need for cache-specific chaining
- Essential for maintaining consistency of derived caches that depend on relation metadata
- Used extensively in logical replication, plan caching, and type caching subsystems
- Callbacks should be prepared to handle both specific relation invalidations and global resets efficiently

## Simplified Source

```c
// Simplified version of CacheRegisterRelcacheCallback
void CacheRegisterRelcacheCallback(RelcacheCallbackFunction func, Datum arg) {
    // Check if we have room for another callback
    if (relcache_callback_count >= MAX_RELCACHE_CALLBACKS)
        elog(FATAL, "out of relcache_callback_list slots");

    // Store the callback function and its argument
    relcache_callback_list[relcache_callback_count].function = func;
    relcache_callback_list[relcache_callback_count].arg = arg;

    // Increment the callback counter
    ++relcache_callback_count;
}
```

Key simplifications made:
- Added descriptive comments for each logical step
- Maintained the essential error checking logic
- Preserved the core registration mechanism
- Kept the simple array-based storage approach
- No simplifications needed as the function is already straightforward