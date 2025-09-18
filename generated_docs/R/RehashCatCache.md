# RehashCatCache

## Location
src/backend/utils/cache/catcache.c: 985 - 1022

## Overview
RehashCatCache enlarges a catalog cache by doubling the number of hash buckets to improve cache performance when the cache becomes heavily loaded.

## Definition


## Detailed Description
RehashCatCache is a static function that performs dynamic rehashing of a catalog cache when it becomes overcrowded. The function doubles the number of hash buckets in the cache and redistributes all existing cache entries across the new bucket array. This operation maintains cache performance by reducing hash collisions as the cache grows in size.

The rehashing process involves:
1. Logging debug information about the rehashing operation
2. Allocating a new hash table with double the number of buckets
3. Moving all cache entries from the old hash table to the new one using their hash values
4. Replacing the old bucket array with the new one
5. Freeing the old bucket array memory

The function uses doubly-linked lists (dlist) to manage cache entries within each bucket, allowing efficient insertion and deletion operations during the rehashing process.

## Parameters / Member Variables
- : Pointer to the CatCache structure that needs to be rehashed

## Dependencies
- Functions called/Symbols referenced:
  - elog (for debug logging)
  - MemoryContextAllocZero (for allocating new bucket array)
  - dlist_foreach_modify (for iterating through cache entries)
  - dlist_container (for accessing CatCTup from list node)
  - HASH_INDEX (macro for computing hash bucket index)
  - dlist_delete (for removing entries from old buckets)
  - dlist_push_head (for adding entries to new buckets)
  - pfree (for freeing old bucket array)
- Called from:
  - CatalogCacheCreateEntry (when cache load factor becomes too high)

## Notes and Other Information
- This is a static function, only accessible within catcache.c
- The function doubles the bucket count each time it's called, following exponential growth
- All existing cache entries are preserved and redistributed during rehashing
- The operation is performed in CacheMemoryContext to ensure proper memory management
- Debug logging helps track cache performance and rehashing frequency
- The rehashing maintains the integrity of hash-based lookups by recalculating bucket assignments