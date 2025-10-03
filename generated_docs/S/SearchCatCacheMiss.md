# SearchCatCacheMiss

## Location
[src/backend/utils/cache/catcache.c:1475-1623](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/catcache.c#L1475-L1623)

## Overview
Handles catalog cache misses by searching the actual catalog tables and creating new cache entries for found tuples or negative entries for non-existent tuples.

## Definition

```c
static pg_noinline HeapTuple
SearchCatCacheMiss(CatCache *cache,
				   int nkeys,
				   uint32 hashValue,
				   Index hashIndex,
				   Datum v1,
				   Datum v2,
				   Datum v3,
				   Datum v4)
```
## Detailed Description
SearchCatCacheMiss is called when SearchCatCacheInternal fails to find a tuple in the cache. It performs the actual database search by opening the catalog relation and scanning it with the provided key values. If a matching tuple is found, it creates a new positive cache entry. If no tuple is found, it creates a negative cache entry to remember that the tuple doesn't exist, avoiding future expensive disk searches.

The function handles several complex scenarios including recursive cache lookups during table access, tuple staleness detection during detoasting, and bootstrap mode considerations. It uses a retry loop to handle cases where tuples become outdated during cache entry creation. The function is explicitly marked as pg_noinline to keep the fast path in SearchCatCacheInternal optimized.

## Parameters / Member Variables
- `*cache`: Pointer to the CatCache structure for the catalog being searched
- `nkeys`: Number of key values being used for the search
- `hashValue`: Pre-computed hash value for the search keys
- `hashIndex`: Hash bucket index where the entry should be placed
- `v1`: First key value (Datum) for the search
- `v2`: Second key value (Datum) for the search
- `v3`: Third key value (Datum) for the search
- `v4`: Fourth key value (Datum) for the search
## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - [table_close](../t/table_close.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [systable_endscan](../s/systable_endscan.md)
  - [CatalogCacheCreateEntry](../C/CatalogCacheCreateEntry.md)
  - [IndexScanOK](../I/IndexScanOK.md)
  - IsBootstrapProcessingMode
  - [ResourceOwnerEnlarge](../R/ResourceOwnerEnlarge.md)
  - [ResourceOwnerRememberCatCacheRef](../R/ResourceOwnerRememberCatCacheRef.md)
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

## Simplified Source

```c
static HeapTuple SearchCatCacheMiss(CatCache *cache, int nkeys, uint32 hashValue,
                                  Index hashIndex, Datum v1, Datum v2, Datum v3, Datum v4) {
    ScanKeyData cur_skey[CATCACHE_MAXKEYS];
    Relation relation;
    SysScanDesc scandesc;
    HeapTuple ntp;
    CatCTup *ct;
    bool stale;
    Datum arguments[4] = {v1, v2, v3, v4};

    // Open catalog relation for scanning
    relation = table_open(cache->cc_reloid, AccessShareLock);

    do {
        // Prepare scan keys with current arguments
        memcpy(cur_skey, cache->cc_skey, sizeof(ScanKeyData) * nkeys);
        cur_skey[0].sk_argument = v1;
        cur_skey[1].sk_argument = v2;
        cur_skey[2].sk_argument = v3;
        cur_skey[3].sk_argument = v4;

        // Begin system catalog scan
        scandesc = systable_beginscan(relation, cache->cc_indexoid,
                                    IndexScanOK(cache, cur_skey), NULL, nkeys, cur_skey);

        ct = NULL;
        stale = false;

        // Search for matching tuple
        while (HeapTupleIsValid(ntp = systable_getnext(scandesc))) {
            // Create cache entry for found tuple
            ct = CatalogCacheCreateEntry(cache, ntp, NULL, hashValue, hashIndex);

            if (ct == NULL) {
                stale = true;  // Tuple became stale, retry needed
                break;
            }

            // Set reference count and track ownership
            ResourceOwnerEnlarge(CurrentResourceOwner);
            ct->refcount++;
            ResourceOwnerRememberCatCacheRef(CurrentResourceOwner, &ct->tuple);
            break;  // Found our tuple
        }

        systable_endscan(scandesc);
    } while (stale);  // Retry if tuple became stale

    table_close(relation, AccessShareLock);

    // Handle case where no tuple was found
    if (ct == NULL) {
        if (IsBootstrapProcessingMode())
            return NULL;  // No negative entries in bootstrap mode

        // Create negative cache entry
        ct = CatalogCacheCreateEntry(cache, NULL, arguments, hashValue, hashIndex);
        return NULL;  // Don't return negative entries to caller
    }

    return &ct->tuple;
}
```