# RehashCatCacheLists

## Location
src/backend/utils/cache/catcache.c: 1023 - 1064

## Overview
RehashCatCacheLists enlarges a catalog cache's list storage by doubling the number of hash buckets used for managing catalog cache lists to improve performance when list storage becomes heavily loaded.

## Definition
```c
static void RehashCatCacheLists(CatCache *cp)
```

## Detailed Description
RehashCatCacheLists is a static function that performs dynamic rehashing of the list storage component of a catalog cache. While RehashCatCache handles individual cache entries, this function specifically manages the rehashing of catalog cache lists (CatCList structures). It doubles the number of hash buckets for list storage and redistributes all existing cache lists across the new bucket array.

The function operates similarly to RehashCatCache but works on the cc_lbucket array instead of cc_bucket. Catalog cache lists are used to store multiple related tuples that match a partial key, providing efficient access to groups of related catalog entries.

The rehashing process involves:
1. Logging debug information about the list rehashing operation
2. Allocating a new hash table with double the number of list buckets
3. Moving all cache lists from the old hash table to the new one using their hash values
4. Replacing the old list bucket array with the new one
5. Freeing the old list bucket array memory

## Parameters / Member Variables
- `cp`: Pointer to the CatCache structure whose list storage needs to be rehashed

## Dependencies
- Functions called/Symbols referenced:
  - elog (for debug logging)
  - MemoryContextAllocZero (for allocating new list bucket array)
  - dlist_foreach_modify (for iterating through cache lists)
  - dlist_container (for accessing CatCList from list node)
  - HASH_INDEX (macro for computing hash bucket index)
  - dlist_delete (for removing lists from old buckets)
  - dlist_push_head (for adding lists to new buckets)
  - pfree (for freeing old list bucket array)
- Called from:
  - SearchCatCacheList (when list cache load factor becomes too high)

## Notes and Other Information
- This is a static function, only accessible within catcache.c
- Works specifically on catalog cache lists (CatCList) rather than individual cache entries
- The function doubles the list bucket count each time it's called
- All existing cache lists are preserved and redistributed during rehashing
- The operation is performed in CacheMemoryContext for proper memory management
- Maintains separate hash tables for individual entries (cc_bucket) and lists (cc_lbucket)
- Debug logging helps monitor list cache performance and rehashing frequency
- Essential for maintaining efficient access to grouped catalog entries