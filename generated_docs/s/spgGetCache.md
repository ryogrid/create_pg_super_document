# spgGetCache

## Location
[src/backend/access/spgist/spgutils.c:182-308](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgutils.c#L182-L308)

## Overview
spgGetCache fetches and initializes the local cache of SP-GiST access method-specific information about an index, creating and configuring the cache if it doesn't already exist.

## Definition

```c
SpGistCache *
spgGetCache(Relation index)
```
## Detailed Description
This function manages the SP-GiST cache (stored in rd_amcache) for an index relation. If the cache doesn't exist, it creates a new SpGistCache structure and populates it with configuration information obtained from the opclass config function, type descriptions for various data types used by the index, and metadata from the index's metapage.

The function performs several key operations during cache initialization:
1. Validates that the index has exactly one key column (SP-GiST requirement)
2. Determines the nominal input data type using GetIndexInputType
3. Calls the opclass config function to get SP-GiST-specific configuration
4. Handles leafType determination, including binary coercion checks
5. Validates compress method requirements when leaf type differs from input type
6. Fills type descriptors for attribute, leaf, prefix, and label types
7. For real (non-partitioned) indexes, reads metadata from the metapage

## Parameters / Member Variables
- `index`: The relation representing the SP-GiST index
## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md) (allocate zeroed cache structure)
  - IndexRelationGetNumberOfKeyAttributes, IndexRelationGetNumberOfAttributes (index validation)
  - [GetIndexInputType](../G/GetIndexInputType.md) (determine nominal input type)
  - [index_getprocinfo](../i/index_getprocinfo.md) (get opclass procedure info)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md) (call opclass config function)
  - [IsBinaryCoercible](../I/IsBinaryCoercible.md) (check type coercion compatibility)
  - [fillTypeDesc](../f/fillTypeDesc.md) (populate type descriptors)
  - [index_getprocid](../i/index_getprocid.md) (check for compress procedure)
  - [ReadBuffer](../R/ReadBuffer.md), LockBuffer, SpGistPageGetMeta (metapage access)
  - Constants: spgKeyColumn, SPGIST_CONFIG_PROC, SPGIST_COMPRESS_PROC, etc.
- Called from (representative examples):
  - [spgcanreturn](spgcanreturn.md) (at src/backend/access/spgist/spgscan.c:1092)
  - [initSpGistState](../i/initSpGistState.md) (at src/backend/access/spgist/spgutils.c:347)
  - [allocNewBuffer](../a/allocNewBuffer.md) (at src/backend/access/spgist/spgutils.c:507)
  - [SpGistGetBuffer](../S/SpGistGetBuffer.md) (at src/backend/access/spgist/spgutils.c:563)
  - [SpGistSetLastUsedPage](../S/SpGistSetLastUsedPage.md) (at src/backend/access/spgist/spgutils.c:667)

## Notes and Other Information
- Located in src/backend/access/spgist/spgutils.c:182-308
- The cache is stored in the relation's rd_amcache field for efficient reuse
- SP-GiST indexes must have exactly one key column but can have INCLUDE columns
- Handles polymorphic opclasses by passing the actual input type to the config function
- Includes validation that compress method is defined when leaf type differs from input type
- For partitioned indexes, skips metapage reading since they don't have physical storage
- The function implements lazy initialization - cache is created only when first needed
- Type descriptors are cached for efficient access during index operations

## Simplified Source

```c
SpGistCache *spgGetCache(Relation index) {
    SpGistCache *cache;

    // Return existing cache if available
    if (index->rd_amcache != NULL) {
        return (SpGistCache *) index->rd_amcache;
    }

    // Create new cache
    cache = MemoryContextAllocZero(index->rd_indexcxt, sizeof(SpGistCache));

    // Validate index structure (SP-GiST requires exactly one key column)
    Assert(IndexRelationGetNumberOfKeyAttributes(index) == 1);

    // Get nominal input data type for polymorphic opclasses
    Oid atttype = GetIndexInputType(index, spgKeyColumn + 1);

    // Call opclass config function
    spgConfigIn in;
    in.attType = atttype;
    FmgrInfo *procinfo = index_getprocinfo(index, 1, SPGIST_CONFIG_PROC);
    FunctionCall2Coll(procinfo,
                      index->rd_indcollation[spgKeyColumn],
                      PointerGetDatum(&in),
                      PointerGetDatum(&cache->config));

    // Set leaf type (use config value or derive from index column)
    if (!OidIsValid(cache->config.leafType)) {
        cache->config.leafType =
            TupleDescAttr(RelationGetDescr(index), spgKeyColumn)->atttypid;

        // Handle binary-coercible types
        if (cache->config.leafType != atttype &&
            IsBinaryCoercible(cache->config.leafType, atttype))
            cache->config.leafType = atttype;
    }

    // Fill type descriptors
    fillTypeDesc(&cache->attType, atttype);

    if (cache->config.leafType != atttype) {
        // Verify compress method exists when types differ
        if (!OidIsValid(index_getprocid(index, 1, SPGIST_COMPRESS_PROC)))
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("compress method must be defined when leaf type is different from input type")));
        fillTypeDesc(&cache->attLeafType, cache->config.leafType);
    } else {
        cache->attLeafType = cache->attType;
    }

    fillTypeDesc(&cache->attPrefixType, cache->config.prefixType);
    fillTypeDesc(&cache->attLabelType, cache->config.labelType);

    // Read metapage for real indexes (not partitioned)
    if (index->rd_rel->relkind != RELKIND_PARTITIONED_INDEX) {
        Buffer metabuffer = ReadBuffer(index, SPGIST_METAPAGE_BLKNO);
        LockBuffer(metabuffer, BUFFER_LOCK_SHARE);
        SpGistMetaPageData *metadata = SpGistPageGetMeta(BufferGetPage(metabuffer));

        if (metadata->magicNumber != SPGIST_MAGIC_NUMBER)
            elog(ERROR, "index \"%s\" is not an SP-GiST index",
                 RelationGetRelationName(index));

        cache->lastUsedPages = metadata->lastUsedPages;
        UnlockReleaseBuffer(metabuffer);
    }

    index->rd_amcache = (void *) cache;
    return cache;
}
```