# InitializeAttoptCache

## Location
[src/backend/utils/cache/attoptcache.c:78-103](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/attoptcache.c#L78-L103)

## Overview
Initializes the attribute options cache system, setting up the hash table structure and registering invalidation callbacks.

## Definition
```c
static void InitializeAttoptCache(void)
```

## Detailed Description
InitializeAttoptCache performs the one-time initialization of PostgreSQL's attribute options caching subsystem. It creates and configures the global AttoptCacheHash hash table that will store cached attribute option data for efficient retrieval. The function sets up the hash table with appropriate sizing (256 initial buckets) and configures it to use HASH_ELEM and HASH_BLOBS flags for proper key and entry management.

The initialization process ensures the CacheMemoryContext is available for storing cached data and registers an invalidation callback (InvalidateAttoptCacheCallback) with the system catalog cache manager. This callback will be triggered whenever the pg_attribute catalog (ATTNUM cache) is modified, ensuring cache consistency.

## Parameters / Member Variables
None - this is a void function with no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - hash_create - Create new hash table with specified parameters
  - CreateCacheMemoryContext - Ensure cache memory context exists
  - CacheRegisterSyscacheCallback - Register invalidation callback for ATTNUM syscache
  - InvalidateAttoptCacheCallback - The callback function to register
- Data structures used:
  - HASHCTL - Hash table control structure for configuration
  - AttoptCacheKey - Key structure for cache entries
  - AttoptCacheEntry - Cache entry structure
  - AttoptCacheHash - Global hash table variable
  - CacheMemoryContext - Memory context for cache allocations
- Constants used:
  - HASH_ELEM - Hash table flag for element-based operations
  - HASH_BLOBS - Hash table flag for binary key comparison
  - ATTNUM - System catalog cache identifier for pg_attribute
- Called from:
  - get_attribute_options - Called on first access if cache not initialized

## Notes and Other Information
- Performs lazy initialization - only called when first attribute options are requested
- Creates hash table with 256 initial buckets for reasonable performance
- Uses HASH_BLOBS flag since AttoptCacheKey may contain padding that should be ignored in comparisons
- Registers for ATTNUM syscache invalidations to maintain consistency with pg_attribute changes
- Part of the broader PostgreSQL caching infrastructure for performance optimization
- Located in src/backend/utils/cache/attoptcache.c as part of the attribute options caching subsystem