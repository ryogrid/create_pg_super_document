# CatalogCacheInitializeCache

## Location
[src/backend/utils/cache/catcache.c:1086-1194](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/catcache.c#L1086-L1194)

## Overview
CatalogCacheInitializeCache initializes a catalog cache structure by opening the target relation, copying its tuple descriptor, and setting up key information for efficient hash-based lookups.

## Definition
```c
static void CatalogCacheInitializeCache(CatCache *cache)
```

## Detailed Description
CatalogCacheInitializeCache is a static function that performs the complete initialization of a catalog cache. This function is called during cache setup to establish all the metadata needed for efficient catalog lookups. The initialization process is critical for ensuring that the cache can properly hash, compare, and store catalog tuples.

The function performs several key operations:
1. Opens the target relation with AccessShareLock to read its metadata
2. Switches to CacheMemoryContext to ensure persistent storage
3. Creates a permanent copy of the relation's tuple descriptor using CreateTupleDescCopyConstr
4. Saves the relation name and shared status for debugging and operational purposes
5. Initializes key information for each cache key column, including:
   - Determining the data type of each key column
   - Setting up hash and equality functions for efficient lookups
   - Configuring scan keys for heap scans and comparisons
   - Ensuring proper collation settings (C collation for cache keys)

The function validates that cache key columns are NOT NULL, as required for reliable hashing and comparison. It also handles the special case where keyno[i] == 0, treating it as an OID column.

## Parameters / Member Variables
- `cache`: Pointer to the CatCache structure to be initialized

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md) (to open the target relation)
  - [CreateTupleDescCopyConstr](CreateTupleDescCopyConstr.md) (to copy tuple descriptor)
  - RelationGetDescr (to get relation descriptor)
  - RelationGetRelationName (to get relation name)
  - RelationGetForm (to get relation form)
  - [pstrdup](../p/pstrdup.md) (to duplicate relation name string)
  - [table_close](../t/table_close.md) (to close the relation)
  - TupleDescAttr (to access tuple descriptor attributes)
  - [GetCCHashEqFuncs](../G/GetCCHashEqFuncs.md) (to get hash and equality functions)
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md) (to initialize function manager info)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (for memory context management)
- Called from:
  - [InitCatCachePhase2](../I/InitCatCachePhase2.md) (during cache system initialization)
  - [SearchCatCacheInternal](../S/SearchCatCacheInternal.md) (lazy initialization during first search)
  - [GetCatCacheHashValue](../G/GetCatCacheHashValue.md) (lazy initialization during hash computation)
  - [SearchCatCacheList](../S/SearchCatCacheList.md) (lazy initialization during list search)
  - [PrepareToInvalidateCacheTuple](../P/PrepareToInvalidateCacheTuple.md) (lazy initialization during invalidation)

## Notes and Other Information
- This is a static function, only accessible within catcache.c
- The function uses lazy initialization - caches are not fully initialized until first use
- All allocations are done in CacheMemoryContext for persistence across transactions
- Cache key columns must be NOT NULL for reliable operation
- System attributes (negative column numbers) are not supported in caches
- The function sets up BTEqualStrategyNumber for all cache key comparisons
- C collation is enforced for all cache keys to ensure consistent behavior
- Debug logging helps track cache initialization and key setup
- The cc_tupdesc field being set marks the cache as fully initialized

## Simplified Source

```c
static void CatalogCacheInitializeCache(CatCache *cache)
{
    Relation relation;
    MemoryContext oldcxt;
    TupleDesc tupdesc;
    int i;

    // Open the target relation for reading metadata
    relation = table_open(cache->cc_reloid, AccessShareLock);

    // Switch to cache memory context for persistent allocations
    Assert(CacheMemoryContext != NULL);
    oldcxt = MemoryContextSwitchTo(CacheMemoryContext);

    // Copy the relation's tuple descriptor permanently
    tupdesc = CreateTupleDescCopyConstr(RelationGetDescr(relation));

    // Save relation name and shared status
    cache->cc_relname = pstrdup(RelationGetRelationName(relation));
    cache->cc_relisshared = RelationGetForm(relation)->relisshared;

    // Return to caller's memory context and close relation
    MemoryContextSwitchTo(oldcxt);
    table_close(relation, AccessShareLock);

    CACHE_elog(DEBUG2, "CatalogCacheInitializeCache: %s, %d keys",
               cache->cc_relname, cache->cc_nkeys);

    // Initialize key information for each cache key
    for (i = 0; i < cache->cc_nkeys; ++i)
    {
        Oid keytype;
        RegProcedure eqfunc;

        if (cache->cc_keyno[i] > 0)
        {
            // Regular column - get type from tuple descriptor
            Form_pg_attribute attr = TupleDescAttr(tupdesc, cache->cc_keyno[i] - 1);
            keytype = attr->atttypid;
            Assert(attr->attnotnull); // Cache keys must be NOT NULL
        }
        else
        {
            // Special case: keyno[i] == 0 means OID column
            if (cache->cc_keyno[i] < 0)
                elog(FATAL, "sys attributes are not supported in caches");
            keytype = OIDOID;
        }

        // Get hash and equality functions for this key type
        GetCCHashEqFuncs(keytype,
                         &cache->cc_hashfunc[i],
                         &eqfunc,
                         &cache->cc_fastequal[i]);

        // Set up function manager info for equality function
        fmgr_info_cxt(eqfunc,
                      &cache->cc_skey[i].sk_func,
                      CacheMemoryContext);

        // Initialize scan key attributes
        cache->cc_skey[i].sk_attno = cache->cc_keyno[i];
        cache->cc_skey[i].sk_strategy = BTEqualStrategyNumber;
        cache->cc_skey[i].sk_subtype = InvalidOid;
        cache->cc_skey[i].sk_collation = C_COLLATION_OID;

        CACHE_elog(DEBUG2, "CatalogCacheInitializeCache %s %d %p",
                   cache->cc_relname, i, cache);
    }

    // Mark cache as fully initialized
    cache->cc_tupdesc = tupdesc;
}
```