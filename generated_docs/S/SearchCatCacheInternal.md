# SearchCatCacheInternal

## Location
src/backend/utils/cache/catcache.c: 1363 - 1474

## Overview
Internal work-horse function for catalog cache searching that handles the core lookup logic for SearchCatCache and SearchCatCacheN functions.

## Definition


## Detailed Description
SearchCatCacheInternal is the core implementation function that performs catalog cache lookups. It handles the complete search process including hash computation, bucket traversal, tuple comparison, and cache management. The function first checks if a tuple matching the given key values exists in the cache. If found, it moves the entry to the front of the hash bucket for better performance and increments its reference count. If not found, it delegates to SearchCatCacheMiss to handle the cache miss scenario.

The function implements an LRU-like optimization by moving frequently accessed cache entries to the front of their hash bucket lists. It also handles both positive cache entries (actual tuples) and negative cache entries (known non-existent tuples) to avoid repeated expensive disk I/O operations.

## Parameters / Member Variables
- : Pointer to the CatCache structure representing the specific catalog cache to search
- : Number of key values being used for the search (must match cache->cc_nkeys)
- : First key value (Datum) for the search
- : Second key value (Datum) for the search
- : Third key value (Datum) for the search  
- : Fourth key value (Datum) for the search

## Dependencies
- Functions called/Symbols referenced:
  - IsTransactionState
  - CatalogCacheInitializeCache
  - CatalogCacheComputeHashValue
  - CatalogCacheCompareTuple
  - SearchCatCacheMiss
  - ResourceOwnerEnlarge
  - ResourceOwnerRememberCatCacheRef
  - dlist_move_head
  - HASH_INDEX
- Called from (representative examples):
  - SearchCatCache
  - SearchCatCache1
  - SearchCatCache2
  - SearchCatCache3
  - SearchCatCache4

## Notes and Other Information
- Requires an active transaction state (checked via IsTransactionState assertion)
- Performs lazy initialization of cache tuple descriptor if needed
- Implements cache statistics tracking when CATCACHE_STATS is enabled
- Handles both positive and negative cache entries efficiently
- Uses doubly-linked lists for hash bucket management with LRU optimization
- Dead cache entries are skipped during traversal
- Reference counting is managed through PostgreSQL's resource owner system