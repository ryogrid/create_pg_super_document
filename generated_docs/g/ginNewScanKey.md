# ginNewScanKey

## Location
[src/backend/access/gin/ginscan.c:268-489](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginscan.c#L268-L489)

## Overview
Initializes scan key structures for a GIN (Generalized Inverted Index) index scan by processing query values and setting up internal data structures for efficient scanning.

## Definition
```c
void ginNewScanKey(IndexScanDesc scan)
```

## Detailed Description
The `ginNewScanKey` function is a core component of the GIN index scanning infrastructure that processes the scan keys provided by the query planner and transforms them into internal GIN-specific data structures. This function performs several critical operations:

1. **Memory Management**: Allocates scan key information in the key context to ensure proper memory lifecycle management
2. **Query Extraction**: Calls the appropriate `extractQueryFn` for each scan key to extract searchable values from the query arguments
3. **Search Mode Processing**: Handles different GIN search modes (DEFAULT, ALL, EVERYTHING) and applies appropriate logic for each
4. **Null Handling**: Processes null query values and creates appropriate null category representations
5. **Key Reorganization**: Reorders exclude-only keys to appear after normal keys for optimal scanning performance
6. **Version Compatibility**: Ensures compatibility with older GIN index versions and provides appropriate error messages

The function supports various search scenarios including exact matches, partial matches, null searches, and full-index scans. It also handles the complex logic around exclude-only operations and ensures that at least one normal scan key exists when exclude-only keys are present.

## Parameters / Member Variables
- `scan`: IndexScanDesc structure containing the index scan information, including the scan keys to be processed and the opaque scan state

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md): Memory context management
  - [FunctionCall7Coll](../F/FunctionCall7Coll.md): Calls the extractQuery function for each scan key
  - [ginFillScanKey](ginFillScanKey.md): Fills in the GIN-specific scan key structure
  - [ginScanKeyAddHiddenEntry](ginScanKeyAddHiddenEntry.md): Adds hidden entries for special search modes
  - [ginGetStats](ginGetStats.md): Retrieves GIN index statistics for version checking
  - `pgstat_count_index_scan`: Updates index scan statistics
- Called from (representative examples):
  - [gingetbitmap](gingetbitmap.md): Main entry point for GIN bitmap scans

## Notes and Other Information
- This function is called at the beginning of each GIN index scan operation
- The function handles backward compatibility with older GIN index versions (version 0) and will error if unsupported operations are attempted on old indexes
- Memory allocated during this function persists until the scan ends or is rescanned
- The function supports complex query patterns including partial matches and exclude-only operations
- Search modes determine how the scan will behave: DEFAULT for normal equality/containment searches, ALL for exclude-only operations, and EVERYTHING for full-index scans
- The function ensures that exclude-only keys are properly positioned after normal keys in the scan key array for correct execution order

## Simplified Source

```c
// Simplified version of ginNewScanKey
void
ginNewScanKey(IndexScanDesc scan)
{
    GinScanOpaque so = (GinScanOpaque) scan->opaque;
    ScanKey scankey = scan->keyData;
    MemoryContext oldCtx;
    bool hasNullQuery = false;
    bool attrHasNormalScan[INDEX_MAX_KEYS] = {false};
    int numExcludeOnly;

    // Switch to key context for allocation
    oldCtx = MemoryContextSwitchTo(so->keyCtx);

    // Allocate scan keys and entries arrays
    so->keys = palloc(Max(scan->numberOfKeys, 1) * sizeof(GinScanKeyData));
    so->nkeys = 0;
    so->totalentries = 0;
    so->allocentries = 32;
    so->entries = palloc(so->allocentries * sizeof(GinScanEntry));
    so->isVoidRes = false;

    // Process each scan key
    for (int i = 0; i < scan->numberOfKeys; i++)
    {
        ScanKey skey = &scankey[i];
        Datum *queryValues;
        int32 nQueryValues = 0;
        bool *partial_matches = NULL;
        Pointer *extra_data = NULL;
        bool *nullFlags = NULL;
        GinNullCategory *categories;
        int32 searchMode = GIN_SEARCH_MODE_DEFAULT;

        // Handle null query argument
        if (skey->sk_flags & SK_ISNULL)
        {
            so->isVoidRes = true;
            break;
        }

        // Extract query values using operator class function
        queryValues = (Datum *) DatumGetPointer(
            FunctionCall7Coll(&so->ginstate.extractQueryFn[skey->sk_attno - 1],
                             so->ginstate.supportCollation[skey->sk_attno - 1],
                             skey->sk_argument,
                             PointerGetDatum(&nQueryValues),
                             UInt16GetDatum(skey->sk_strategy),
                             PointerGetDatum(&partial_matches),
                             PointerGetDatum(&extra_data),
                             PointerGetDatum(&nullFlags),
                             PointerGetDatum(&searchMode)));

        // Validate search mode
        if (searchMode < GIN_SEARCH_MODE_DEFAULT || searchMode > GIN_SEARCH_MODE_ALL)
            searchMode = GIN_SEARCH_MODE_ALL;

        if (searchMode != GIN_SEARCH_MODE_DEFAULT)
            hasNullQuery = true;

        // Handle case where no query values extracted
        if (queryValues == NULL || nQueryValues <= 0)
        {
            if (searchMode == GIN_SEARCH_MODE_DEFAULT)
            {
                so->isVoidRes = true;
                break;
            }
            nQueryValues = 0;
        }

        // Create null category representation
        categories = palloc0(nQueryValues * sizeof(GinNullCategory));
        if (nullFlags)
        {
            for (int j = 0; j < nQueryValues; j++)
            {
                if (nullFlags[j])
                {
                    categories[j] = GIN_CAT_NULL_KEY;
                    hasNullQuery = true;
                }
            }
        }

        // Fill in the scan key structure
        ginFillScanKey(so, skey->sk_attno, skey->sk_strategy, searchMode,
                      skey->sk_argument, nQueryValues, queryValues, categories,
                      partial_matches, extra_data);

        // Track normal scan keys per attribute
        if (searchMode != GIN_SEARCH_MODE_ALL)
            attrHasNormalScan[skey->sk_attno - 1] = true;
    }

    // Handle exclude-only keys - ensure each attribute has at least one normal key
    numExcludeOnly = 0;
    for (int i = 0; i < so->nkeys; i++)
    {
        GinScanKey key = &so->keys[i];
        if (key->searchMode == GIN_SEARCH_MODE_ALL)
        {
            if (!attrHasNormalScan[key->attnum - 1])
            {
                key->excludeOnly = false;
                ginScanKeyAddHiddenEntry(so, key, GIN_CAT_EMPTY_QUERY);
                attrHasNormalScan[key->attnum - 1] = true;
            }
            else
                numExcludeOnly++;
        }
    }

    // Reorder keys: normal keys first, then exclude-only keys
    if (numExcludeOnly > 0)
    {
        GinScanKey tmpkeys = palloc(so->nkeys * sizeof(GinScanKeyData));
        int normalIdx = 0;
        int excludeIdx = so->nkeys - numExcludeOnly;

        for (int i = 0; i < so->nkeys; i++)
        {
            if (so->keys[i].excludeOnly)
                memcpy(tmpkeys + excludeIdx++, &so->keys[i], sizeof(GinScanKeyData));
            else
                memcpy(tmpkeys + normalIdx++, &so->keys[i], sizeof(GinScanKeyData));
        }
        memcpy(so->keys, tmpkeys, so->nkeys * sizeof(GinScanKeyData));
        pfree(tmpkeys);
    }

    // Generate EVERYTHING scan key if no regular keys
    if (so->nkeys == 0 && !so->isVoidRes)
    {
        hasNullQuery = true;
        ginFillScanKey(so, FirstOffsetNumber, InvalidStrategy, GIN_SEARCH_MODE_EVERYTHING,
                      (Datum) 0, 0, NULL, NULL, NULL, NULL);
    }

    // Check index version compatibility for null queries
    if (hasNullQuery && !so->isVoidRes)
    {
        GinStatsData ginStats;
        ginGetStats(scan->indexRelation, &ginStats);
        if (ginStats.ginVersion < 1)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                          errmsg("old GIN indexes do not support whole-index scans nor searches for nulls")));
    }

    MemoryContextSwitchTo(oldCtx);
    pgstat_count_index_scan(scan->indexRelation);
}
```