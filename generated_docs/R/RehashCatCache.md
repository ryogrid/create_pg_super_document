# RehashCatCache

## Location
[src/backend/utils/cache/catcache.c:985-1022](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/catcache.c#L985-L1022)

## Overview
RehashCatCache enlarges a catalog cache by doubling the number of hash buckets to improve cache performance when the cache becomes heavily loaded.

## Definition

```c
static void
RehashCatCache(CatCache *cp)
```
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
- `*cp`: Pointer to the CatCache structure that needs to be rehashed
## Dependencies
- Functions called/Symbols referenced:
  - elog (for debug logging)
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md) (for allocating new bucket array)
  - dlist_foreach_modify (for iterating through cache entries)
  - dlist_container (for accessing CatCTup from list node)
  - HASH_INDEX (macro for computing hash bucket index)
  - [dlist_delete](../d/dlist_delete.md) (for removing entries from old buckets)
  - [dlist_push_head](../d/dlist_push_head.md) (for adding entries to new buckets)
  - [pfree](../p/pfree.md) (for freeing old bucket array)
- Called from:
  - [CatalogCacheCreateEntry](../C/CatalogCacheCreateEntry.md) (when cache load factor becomes too high)

## Notes and Other Information
- This is a static function, only accessible within catcache.c
- The function doubles the bucket count each time it's called, following exponential growth
- All existing cache entries are preserved and redistributed during rehashing
- The operation is performed in CacheMemoryContext to ensure proper memory management
- Debug logging helps track cache performance and rehashing frequency
- The rehashing maintains the integrity of hash-based lookups by recalculating bucket assignments

## Simplified Source

```c
static void
RehashCatCache(CatCache *cp)
{
    dlist_head *newbucket;
    int newnbuckets;
    int i;

    // Log rehashing operation for debugging
    elog(DEBUG1, "rehashing catalog cache id %d for %s; %d tups, %d buckets",
         cp->id, cp->cc_relname, cp->cc_ntup, cp->cc_nbuckets);

    // Create new hash table with double the buckets
    newnbuckets = cp->cc_nbuckets * 2;
    newbucket = (dlist_head *) MemoryContextAllocZero(CacheMemoryContext,
                                                      newnbuckets * sizeof(dlist_head));

    // Move all entries from old table to new table
    for (i = 0; i < cp->cc_nbuckets; i++) {
        dlist_mutable_iter iter;

        dlist_foreach_modify(iter, &cp->cc_bucket[i]) {
            CatCTup *ct = dlist_container(CatCTup, cache_elem, iter.cur);
            int hashIndex = HASH_INDEX(ct->hash_value, newnbuckets);

            // Remove from old bucket and add to new bucket
            dlist_delete(iter.cur);
            dlist_push_head(&newbucket[hashIndex], &ct->cache_elem);
        }
    }

    // Replace old bucket array with new one
    pfree(cp->cc_bucket);
    cp->cc_nbuckets = newnbuckets;
    cp->cc_bucket = newbucket;
}
```