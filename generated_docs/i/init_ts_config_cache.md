# init_ts_config_cache

## Location
[src/backend/utils/cache/ts_cache.c:362-384](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/ts_cache.c#L362-L384)

## Overview
Initializes the text search configuration cache hash table and registers syscache callbacks to maintain cache consistency across multiple system catalogs.

## Definition

```c
static void
init_ts_config_cache(void)
```
## Detailed Description
init_ts_config_cache is a helper function that sets up the infrastructure for caching text search configurations. This function was extracted from lookup_ts_config_cache to allow for early initialization of the cache system, particularly to enable callback registration before caching the current configuration (TSCurrentConfigCache).

The function creates a hash table with 16 initial buckets (larger than parser and dictionary caches) and registers syscache callbacks for both pg_ts_config and pg_ts_config_map catalogs. This dual callback registration is necessary because text search configurations depend on both the configuration definitions and their mapping to dictionaries. Any changes to either catalog must invalidate the entire configuration cache to maintain consistency.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [hash_create](../h/hash_create.md) (creates the configuration cache hash table)
  - [CacheRegisterSyscacheCallback](../C/CacheRegisterSyscacheCallback.md) (registers cache invalidation callbacks for both config and config_map catalogs)
  - [InvalidateTSCacheCallBack](../I/InvalidateTSCacheCallBack.md) (cache invalidation callback function)
  - [CreateCacheMemoryContext](../C/CreateCacheMemoryContext.md) (ensures cache memory context exists)
  - HASHCTL (hash table control structure)
  - TSConfigCacheEntry (configuration cache entry structure)
  - TSConfigCacheHash (global hash table variable)
  - HASH_ELEM, HASH_BLOBS (hash table creation flags)
  - TSCONFIGOID, TSCONFIGMAP (syscache identifiers)
- Called from (representative examples):
  - [lookup_ts_config_cache](../l/lookup_ts_config_cache.md) (main configuration lookup function)
  - [getTSCurrentConfig](../g/getTSCurrentConfig.md) (current configuration retrieval function)

## Notes and Other Information
- This is a static function, only visible within the ts_cache.c compilation unit
- The function uses 16 initial buckets, reflecting the potentially larger number of text search configurations compared to parsers (4 buckets) or dictionaries (8 buckets)
- Dual callback registration for both TSCONFIGOID and TSCONFIGMAP is essential because configurations depend on mappings between token types and dictionaries
- The separation of initialization from lookup allows for early cache setup, which is particularly important for caching the current default configuration
- This function must be called before any configuration caching operations to ensure proper callback registration and cache invalidation behavior
- The cache uses CacheMemoryContext for long-lived storage that persists across transactions