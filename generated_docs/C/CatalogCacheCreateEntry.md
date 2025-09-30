# CatalogCacheCreateEntry

## Location
[src/backend/utils/cache/catcache.c:2113-2260](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/catcache.c#L2113-L2260)

## Overview
Creates a new catalog cache entry (CatCTup) from a HeapTuple or cache keys, handling both positive and negative cache entries with proper memory management.

## Definition

```c
static CatCTup *
CatalogCacheCreateEntry(CatCache *cache, HeapTuple ntp, Datum *arguments,
						uint32 hashValue, Index hashIndex)
```
## Detailed Description
CatalogCacheCreateEntry is responsible for creating new entries in the catalog cache. It handles two types of entries: positive entries (from actual tuples found in system catalogs) and negative entries (recording that a search found no matching tuple).

For positive entries, the function processes the provided HeapTuple by flattening any TOAST-ed (out-of-line) values to protect against stale references. It allocates memory for both the CatCTup structure and the tuple data in one contiguous block in CacheMemoryContext, then extracts and stores the key values from the tuple.

For negative entries (when ntp is NULL), it creates a minimal CatCTup with just the search keys copied using CatCacheCopyKeys.

The function includes sophisticated handling of concurrent invalidations through the "in-progress" mechanism and includes optional random failure injection in debug builds to test retry logic.

## Parameters
- : The catalog cache to create the entry in
- : The HeapTuple to cache (NULL for negative entries)
- : Cache keys to use for negative entries (unused for positive entries)
- : Pre-computed hash value for the entry
- : Hash bucket index where the entry will be placed

## Dependencies
- Functions called/Symbols referenced:
  - [toast_flatten_tuple](../t/toast_flatten_tuple.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [heap_getattr](../h/heap_getattr.md)
  - [CatCacheCopyKeys](CatCacheCopyKeys.md)
  - [dlist_push_head](../d/dlist_push_head.md)
  - [RehashCatCache](../R/RehashCatCache.md)
  - HeapTupleHasExternal
  - [pg_prng_uint32](../p/pg_prng_uint32.md) (debug builds only)
- Called from (representative examples):
  - [SearchCatCacheMiss](../S/SearchCatCacheMiss.md)
  - [SearchCatCacheList](../S/SearchCatCacheList.md)

## Notes and Other Information
- Returns NULL if the tuple becomes stale during TOAST decompression (caller must retry)
- Creates entries with initial refcount of 0 - caller must increment if needed
- Automatically triggers hash table expansion when load factor exceeds 2
- Includes random failure injection in debug builds to test error paths
- Static function - only callable from within catcache.c
- Memory allocated in CacheMemoryContext for persistence across transactions
- Handles both by-value and by-reference key types correctly
- Part of PostgreSQL's catalog cache invalidation and consistency mechanism

## Simplified Source

```c
static CatCTup * CatalogCacheCreateEntry(CatCache *cache, HeapTuple ntp, Datum *arguments,
                                       uint32 hashValue, Index hashIndex) {
    CatCTup *ct;
    MemoryContext oldcxt;

    if (ntp) {
        // Creating positive cache entry from tuple
        HeapTuple dtp = ntp;

        // Flatten TOAST values to prevent stale references
        if (HeapTupleHasExternal(ntp)) {
            // Set up invalidation tracking during TOAST access
            CatCInProgress in_progress_ent;
            in_progress_ent.cache = cache;
            in_progress_ent.hash_value = hashValue;
            in_progress_ent.dead = false;

            dtp = toast_flatten_tuple(ntp, cache->cc_tupdesc);

            // Check if entry became invalid during TOAST access
            if (in_progress_ent.dead) {
                heap_freetuple(dtp);
                return NULL;  // Caller must retry
            }
        }

        // Allocate CatCTup and tuple data in one block
        oldcxt = MemoryContextSwitchTo(CacheMemoryContext);
        ct = (CatCTup *) palloc(sizeof(CatCTup) + MAXIMUM_ALIGNOF + dtp->t_len);

        // Copy tuple structure and data
        ct->tuple.t_len = dtp->t_len;
        ct->tuple.t_self = dtp->t_self;
        ct->tuple.t_tableOid = dtp->t_tableOid;
        ct->tuple.t_data = (HeapTupleHeader) MAXALIGN(((char *) ct) + sizeof(CatCTup));
        memcpy((char *) ct->tuple.t_data, (const char *) dtp->t_data, dtp->t_len);

        MemoryContextSwitchTo(oldcxt);

        if (dtp != ntp) {
            heap_freetuple(dtp);
        }

        // Extract cache keys from tuple
        for (int i = 0; i < cache->cc_nkeys; i++) {
            bool isnull;
            Datum atp = heap_getattr(&ct->tuple, cache->cc_keyno[i],
                                   cache->cc_tupdesc, &isnull);
            Assert(!isnull);
            ct->keys[i] = atp;
        }
    } else {
        // Creating negative cache entry with provided keys
        oldcxt = MemoryContextSwitchTo(CacheMemoryContext);
        ct = (CatCTup *) palloc(sizeof(CatCTup));

        // Copy cache keys for negative entry
        CatCacheCopyKeys(cache->cc_tupdesc, cache->cc_nkeys, cache->cc_keyno,
                        arguments, ct->keys);
        MemoryContextSwitchTo(oldcxt);
    }

    // Initialize CatCTup header and add to cache
    ct->ct_magic = CT_MAGIC;
    ct->my_cache = cache;
    ct->c_list = NULL;
    ct->refcount = 0;
    ct->dead = false;
    ct->negative = (ntp == NULL);
    ct->hash_value = hashValue;

    // Add to hash bucket and update counters
    dlist_push_head(&cache->cc_bucket[hashIndex], &ct->cache_elem);
    cache->cc_ntup++;
    CacheHdr->ch_ntup++;

    // Expand hash table if too full (load factor > 2)
    if (cache->cc_ntup > cache->cc_nbuckets * 2) {
        RehashCatCache(cache);
    }

    return ct;
}
```