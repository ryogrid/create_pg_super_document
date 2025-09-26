# InvalidateAttoptCacheCallback

## Location
[src/backend/utils/cache/attoptcache.c:55-77](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/attoptcache.c#L55-L77)

## Overview
A cache invalidation callback function that flushes all cached attribute options when the pg_attribute system catalog is updated.

## Definition
```c
static void InvalidateAttoptCacheCallback(Datum arg, int cacheid, uint32 hashvalue)
```

## Detailed Description
InvalidateAttoptCacheCallback serves as the invalidation mechanism for the attribute options cache (AttoptCacheHash). When PostgreSQL's pg_attribute system catalog is modified, this callback ensures data consistency by flushing all cached attribute option entries. The function takes a conservative approach - instead of selectively invalidating only the affected attribute, it clears the entire cache. This design choice is acceptable because attribute options are not used in performance-critical query execution paths.

The function iterates through all entries in the global AttoptCacheHash using hash table sequential scan operations, properly deallocates memory for cached option data, and removes each entry from the hash table with error checking to detect hash table corruption.

## Parameters / Member Variables
- `arg`: Callback argument passed during registration (unused in this implementation)
- `cacheid`: System catalog cache ID that triggered the invalidation
- `hashvalue`: Hash value of the invalidated item (unused as entire cache is flushed)

## Dependencies
- Functions called/Symbols referenced:
  - [hash_seq_init](../h/hash_seq_init.md) - [Initialize](Initialize.md) sequential hash table scan
  - [hash_seq_search](../h/hash_seq_search.md) - Get next entry in hash table scan
  - [pfree](../p/pfree.md) - Free dynamically allocated memory
  - [hash_search](../h/hash_search.md) (HASH_REMOVE) - Remove entry from hash table
  - elog - Log error messages
- Data structures used:
  - [HASH_SEQ_STATUS](../H/HASH_SEQ_STATUS.md) - [Hash](../H/Hash.md) table sequential scan status
  - AttoptCacheEntry - Cache entry structure
  - AttoptCacheHash - Global attribute options cache hash table
- Called from:
  - [InitializeAttoptCache](InitializeAttoptCache.md) - Registered as syscache callback

## Notes and Other Information
- Registered as a callback for ATTNUM system cache invalidations
- Uses a "flush all" strategy rather than selective invalidation for simplicity
- Includes hash table corruption detection with ERROR logging
- Part of PostgreSQL's attribute options caching subsystem located in src/backend/utils/cache/attoptcache.c
- The conservative invalidation approach is justified by the non-critical performance requirements of attribute options