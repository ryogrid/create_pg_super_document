# CatCacheRemoveCTup

## Location
[src/backend/utils/cache/catcache.c:528-569](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/catcache.c#L528-L569)

## Overview
CatCacheRemoveCTup is a static function that removes and deallocates a catalog cache tuple entry, handling both individual entries and their associated lists.

## Definition

```c
static void
CatCacheRemoveCTup(CatCache *cache, CatCTup *ct)
```
## Detailed Description
This function performs the complete removal of a catalog cache tuple entry from the cache system. It handles the complex cleanup process including deallocation of memory, unlinking from data structures, and maintaining cache statistics. The function includes special logic for handling entries that are part of CatCList structures.

Key operations performed:
- Validates that the entry has zero reference count before removal
- Handles recursive removal of associated CatCList if present
- Delinks the entry from the cache's doubly-linked list
- Frees memory for negative entries' keys (positive entries have keys embedded in tuple data)
- Updates cache tuple counters

The function ensures data integrity by requiring zero reference counts, preventing premature deletion of actively used cache entries.

## Parameters / Member Variables
- `*cache`: Pointer to the CatCache structure containing the entry to be removed
- `*ct`: Pointer to the CatCTup (catalog cache tuple) entry to be removed
## Dependencies
- Functions called/Symbols referenced:
  - [CatCacheRemoveCList](CatCacheRemoveCList.md) (for removing associated cache lists)
  - [dlist_delete](../d/dlist_delete.md) (for unlinking from doubly-linked list)
  - [CatCacheFreeKeys](CatCacheFreeKeys.md) (for deallocating negative entry keys)
  - [pfree](../p/pfree.md) (for memory deallocation)
- Called from (representative examples):
  - [CatCacheRemoveCList](CatCacheRemoveCList.md)
  - [CatCacheInvalidate](CatCacheInvalidate.md)
  - [ResetCatalogCache](../R/ResetCatalogCache.md)
  - [ReleaseCatCacheWithOwner](../R/ReleaseCatCacheWithOwner.md)
  - [SearchCatCacheList](../S/SearchCatCacheList.md)

## Notes and Other Information
- Function is declared as static, making it internal to the catcache.c module
- Includes assertions to ensure the entry has zero reference count and belongs to the specified cache
- Uses a "dead" flag mechanism to prevent infinite recursion when removing entries from CatCLists
- Distinguishes between positive and negative cache entries for proper key memory management
- Decrements both cache-specific and global tuple counters to maintain accurate statistics
- Critical for maintaining cache consistency during invalidation and cleanup operations

## Simplified Source

```c
static void
CatCacheRemoveCTup(CatCache *cache, CatCTup *ct)
{
    // Validate preconditions
    Assert(ct->refcount == 0);
    Assert(ct->my_cache == cache);

    // Handle associated cache list if present
    if (ct->c_list) {
        // Mark as dead and remove the list (which will recursively remove this entry)
        ct->dead = true;
        CatCacheRemoveCList(cache, ct->c_list);
        return;
    }

    // Remove from cache's linked list
    dlist_delete(&ct->cache_elem);

    // Free keys for negative entries (positive entries have embedded keys)
    if (ct->negative)
        CatCacheFreeKeys(cache->cc_tupdesc, cache->cc_nkeys,
                         cache->cc_keyno, ct->keys);

    // Free the entry and update counters
    pfree(ct);
    --cache->cc_ntup;
    --CacheHdr->ch_ntup;
}
```