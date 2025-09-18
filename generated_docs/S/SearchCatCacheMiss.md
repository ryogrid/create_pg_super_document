# SearchCatCacheMiss

## Location
[src/backend/utils/cache/catcache.c:1475-1623](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/catcache.c#L1475-L1623)

## Overview
Handles catalog cache misses by searching the actual catalog tables and creating new cache entries for found tuples or negative entries for non-existent tuples.

## Definition


## Detailed Description
SearchCatCacheMiss is called when SearchCatCacheInternal fails to find a tuple in the cache. It performs the actual database search by opening the catalog relation and scanning it with the provided key values. If a matching tuple is found, it creates a new positive cache entry. If no tuple is found, it creates a negative cache entry to remember that the tuple doesn't exist, avoiding future expensive disk searches.

The function handles several complex scenarios including recursive cache lookups during table access, tuple staleness detection during detoasting, and bootstrap mode considerations. It uses a retry loop to handle cases where tuples become outdated during cache entry creation. The function is explicitly marked as pg_noinline to keep the fast path in SearchCatCacheInternal optimized.

## Parameters / Member Variables
- : Pointer to the CatCache structure for the catalog being searched
- : Number of key values being used for the search
- : Pre-computed hash value for the search keys
- : Hash bucket index where the entry should be placed
- : First key value (Datum) for the search
- : Second key value (Datum) for the search
- : Third key value (Datum) for the search
- : Fourth key value (Datum) for the search

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - table_close
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [systable_endscan](../s/systable_endscan.md)
  - [CatalogCacheCreateEntry](../C/CatalogCacheCreateEntry.md)
  - [IndexScanOK](../I/IndexScanOK.md)
  - IsBootstrapProcessingMode
  - ResourceOwnerEnlarge
  - ResourceOwnerRememberCatCacheRef
  - CACHE_elog
- Called from (representative examples):
  - [SearchCatCacheInternal](SearchCatCacheInternal.md)

## Notes and Other Information
- Explicitly marked pg_noinline to avoid inlining and keep the fast path optimized
- Handles recursive cache lookups that can occur during table_open() due to shared cache invalidation messages
- Implements retry logic to handle tuple staleness during detoasting operations
- Creates negative cache entries for non-existent tuples except in bootstrap mode
- Uses AccessShareLock for safe concurrent access to catalog tables
- Supports both index and sequential scans depending on available indexes
- Immediately sets reference count to 1 for found entries to track usage
- Updates cache statistics when CATCACHE_STATS is enabled