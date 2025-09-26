# CacheRegisterSyscacheCallback

## Location
[src/backend/utils/cache/inval.c:1519-1560](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/inval.c#L1519-L1560)

## Overview
Registers a callback function to be invoked for all future invalidation events in a specified system cache.

## Definition
```c
void CacheRegisterSyscacheCallback(int cacheid, SyscacheCallbackFunction func, Datum arg)
```

## Detailed Description
CacheRegisterSyscacheCallback allows subsystems to register callback functions that will be automatically invoked whenever invalidation events occur in specific system caches. This provides a mechanism for maintaining derived data structures or secondary caches that depend on system catalog information.

Key characteristics:
- **Cache-specific registration**: Each callback is associated with a specific cache ID
- **Chain management**: Multiple callbacks can be registered for the same cache, forming a linked chain where older callbacks are called first
- **Hash value notification**: The callback receives both the cache ID and the hash value of the invalidated tuple
- **Reset handling**: Hash value zero indicates a cache reset request, requiring callbacks to flush all cached state
- **Limited capacity**: The system supports up to MAX_SYSCACHE_CALLBACKS total callbacks across all caches

The function maintains a global callback list and uses a link array to organize callbacks by cache ID. When registering the first callback for a cache, it creates a new chain; subsequent callbacks are appended to maintain call order.

## Parameters / Member Variables
- `cacheid`: Integer identifier of the system cache to monitor (must be valid cache ID < SysCacheSize)
- `func`: SyscacheCallbackFunction pointer to the callback function to invoke on invalidation events
- `arg`: Datum argument to pass to the callback function when it's invoked

## Dependencies
- Functions called/Symbols referenced:
  - MAX_SYSCACHE_CALLBACKS (maximum callback limit constant)
  - SysCacheSize (global variable for cache count validation)
  - syscache_callback_count (global counter for registered callbacks)
  - syscache_callback_links (array mapping cache IDs to callback chains)
  - syscache_callback_list (global array storing callback information)
  - elog (error logging function)
- Called from (representative examples):
  - [InitializeSearchPath](../I/InitializeSearchPath.md) (src/backend/catalog/namespace.c:4766,4771,4776,4781)
  - [lookup_proof_cache](../l/lookup_proof_cache.md) (src/backend/optimizer/util/predtest.c:2128)
  - [find_oper_cache_entry](../f/find_oper_cache_entry.md) (src/backend/parser/parse_oper.c:996,999)
  - [InitPlanCache](../I/InitPlanCache.md) (src/backend/utils/cache/plancache.c:158-164)
  - [lookup_type_cache](../l/lookup_type_cache.md) (src/backend/utils/cache/typcache.c:363-365)

## Notes and Other Information
- The callback registration is permanent for the lifetime of the backend process
- Callbacks are invoked in registration order (older callbacks first) when invalidation occurs
- [Hash](../H/Hash.md) value zero has special meaning (cache reset) and may occasionally create false matches with actual zero hash values
- Most callback implementations handle cache resets by flushing all cached state regardless of the hash value
- Essential for maintaining consistency of derived caches that depend on system catalog data
- Used extensively throughout PostgreSQL for coordinating cache invalidation across subsystems