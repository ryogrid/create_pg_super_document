# SearchCatCacheList

## Location
[src/backend/utils/cache/catcache.c:1697-2072](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/catcache.c#L1697-L2072)

## Overview
Generates a list of all tuples matching a partial key in the catalog cache, supporting searches on just the first K of the cache's N key columns.

## Definition

```c
structing the CatCList.  ctlist must be valid throughout
	 * the PG_TRY block.
	 */
	ctlist = NIL;
```
## Detailed Description
SearchCatCacheList is a core catalog cache function that performs partial key searches to find multiple matching tuples. Unlike SearchCatCache which finds a single tuple using a complete key, this function takes fewer key values and returns all tuples that match the specified prefix of keys.

The function first checks if a matching CatCList already exists in the cache. If found, it moves the list to the front of its hash bucket for faster future access and returns it with an incremented reference count. If not found, it builds a new list by scanning the underlying system catalog, creating or reusing cache entries for each matching tuple.

The implementation includes sophisticated concurrency handling through an "in-progress" mechanism that tracks ongoing list construction to handle cache invalidations that occur during the scan. If invalidation occurs mid-build, the scan is restarted to ensure consistency.

## Parameters
- : The catalog cache to search in
- : Number of key columns to match (must be > 0 and < cache->cc_nkeys)  
- : Value for the first key column
- : Value for the second key column (or dummy value if nkeys < 2)
- : Value for the third key column (or dummy value if nkeys < 3)

## Dependencies
- Functions called/Symbols referenced:
  - [CatalogCacheInitializeCache](../C/CatalogCacheInitializeCache.md)
  - [CatalogCacheComputeHashValue](../C/CatalogCacheComputeHashValue.md)
  - [CatalogCacheCompareTuple](../C/CatalogCacheCompareTuple.md)
  - [CatalogCacheCreateEntry](../C/CatalogCacheCreateEntry.md)
  - [CatCacheCopyKeys](../C/CatCacheCopyKeys.md)
  - [RehashCatCacheLists](../R/RehashCatCacheLists.md)
  - [ResourceOwnerEnlarge](../R/ResourceOwnerEnlarge.md)
  - [ResourceOwnerRememberCatCacheListRef](../R/ResourceOwnerRememberCatCacheListRef.md)
  - [systable_beginscan](../s/systable_beginscan.md)/systable_getnext
  - [table_open](../t/table_open.md)/table_close
- Called from (representative examples):
  - [SearchSysCacheList](SearchSysCacheList.md)
  - Various high-level catalog access functions

## Notes and Other Information
- The caller must not modify the returned list or its tuples
- The caller must call ReleaseCatCacheList() when done with the list
- Automatically handles hash table resizing when the list cache becomes too full
- Uses LRU-style ordering by moving found lists to the front of their hash buckets
- Includes comprehensive error handling with PG_TRY/PG_CATCH blocks
- Supports both index and sequential scans depending on the underlying catalog structure
- The returned list is ordered if an index scan was used during construction

## Simplified Source

```c
CatCList *SearchCatCacheList(CatCache *cache, int nkeys, Datum v1, Datum v2, Datum v3) {
    // Initialize cache if needed
    if (unlikely(cache->cc_tupdesc == NULL))
        CatalogCacheInitializeCache(cache);

    // Set up arguments array
    Datum arguments[CATCACHE_MAXKEYS] = {v1, v2, v3, 0};

    // Initialize or resize list bucket array if needed
    if (cache->cc_lbucket == NULL) {
        int nbuckets = 16;
        cache->cc_lbucket = MemoryContextAllocZero(CacheMemoryContext,
                                                  nbuckets * sizeof(dlist_head));
        cache->cc_nlbuckets = nbuckets;
    } else if (cache->cc_nlist > cache->cc_nlbuckets * 2) {
        RehashCatCacheLists(cache);
    }

    // Calculate hash and search existing lists
    uint32 lHashValue = CatalogCacheComputeHashValue(cache, nkeys, v1, v2, v3, 0);
    Index lHashIndex = HASH_INDEX(lHashValue, cache->cc_nlbuckets);
    dlist_head *lbucket = &cache->cc_lbucket[lHashIndex];

    // Look for existing matching list
    dlist_iter iter;
    dlist_foreach(iter, lbucket) {
        CatCList *cl = dlist_container(CatCList, cache_elem, iter.cur);

        if (cl->dead || cl->hash_value != lHashValue || cl->nkeys != nkeys)
            continue;

        if (!CatalogCacheCompareTuple(cache, nkeys, cl->keys, arguments))
            continue;

        // Found match - move to front and return
        dlist_move_head(lbucket, &cl->cache_elem);
        ResourceOwnerEnlarge(CurrentResourceOwner);
        cl->refcount++;
        ResourceOwnerRememberCatCacheListRef(CurrentResourceOwner, cl);
        return cl;
    }

    // List not found - build new one by scanning catalog
    List *ctlist = NIL;
    bool ordered;

    // Set up invalidation tracking during scan
    CatCInProgress in_progress_ent = {
        .next = catcache_in_progress_stack,
        .cache = cache,
        .hash_value = lHashValue,
        .list = true,
        .dead = false
    };
    catcache_in_progress_stack = &in_progress_ent;

    PG_TRY();
    {
        Relation relation = table_open(cache->cc_reloid, AccessShareLock);

        // Scan table for matching entries (retry if invalidated)
        do {
            // Release previous iteration's refcounts
            foreach(ctlist_item, ctlist) {
                CatCTup *ct = (CatCTup *) lfirst(ctlist_item);
                ct->refcount--;
            }
            ctlist = NIL;
            in_progress_ent.dead = false;

            // Set up scan keys
            ScanKeyData cur_skey[CATCACHE_MAXKEYS];
            memcpy(cur_skey, cache->cc_skey, sizeof(ScanKeyData) * cache->cc_nkeys);
            cur_skey[0].sk_argument = v1;
            cur_skey[1].sk_argument = v2;
            cur_skey[2].sk_argument = v3;

            SysScanDesc scandesc = systable_beginscan(relation, cache->cc_indexoid,
                                                    IndexScanOK(cache, cur_skey),
                                                    NULL, nkeys, cur_skey);
            ordered = (scandesc->irel != NULL);

            // Process each matching tuple
            HeapTuple ntp;
            while (HeapTupleIsValid(ntp = systable_getnext(scandesc)) &&
                   !in_progress_ent.dead) {

                // Find or create cache entry for this tuple
                CatCTup *ct = /* find existing or create new cache entry */;

                ctlist = lappend(ctlist, ct);
                ct->refcount++;
            }

            systable_endscan(scandesc);
        } while (in_progress_ent.dead);

        table_close(relation, AccessShareLock);

        // Build final CatCList structure
        int nmembers = list_length(ctlist);
        MemoryContext oldcxt = MemoryContextSwitchTo(CacheMemoryContext);
        CatCList *cl = palloc(offsetof(CatCList, members) + nmembers * sizeof(CatCTup *));

        CatCacheCopyKeys(cache->cc_tupdesc, nkeys, cache->cc_keyno, arguments, cl->keys);
        MemoryContextSwitchTo(oldcxt);

        // Initialize list properties
        cl->cl_magic = CL_MAGIC;
        cl->my_cache = cache;
        cl->refcount = 0;
        cl->dead = false;
        cl->ordered = ordered;
        cl->nkeys = nkeys;
        cl->hash_value = lHashValue;
        cl->n_members = nmembers;

        // Link members to list
        int i = 0;
        foreach(ctlist_item, ctlist) {
            CatCTup *ct = (CatCTup *) lfirst(ctlist_item);
            cl->members[i++] = ct;
            ct->c_list = cl;
            ct->refcount--;
            if (ct->dead) cl->dead = true;
        }

        // Add to cache and return
        dlist_push_head(lbucket, &cl->cache_elem);
        cache->cc_nlist++;
        cl->refcount++;
        ResourceOwnerRememberCatCacheListRef(CurrentResourceOwner, cl);

        return cl;
    }
    PG_CATCH();
    {
        // Cleanup on error: restore stack and release refcounts
        catcache_in_progress_stack = save_in_progress;
        foreach(ctlist_item, ctlist) {
            CatCTup *ct = (CatCTup *) lfirst(ctlist_item);
            ct->refcount--;
            if (ct->dead && ct->refcount == 0 &&
                (ct->c_list == NULL || ct->c_list->refcount == 0))
                CatCacheRemoveCTup(cache, ct);
        }
        PG_RE_THROW();
    }
    PG_END_TRY();
}
```