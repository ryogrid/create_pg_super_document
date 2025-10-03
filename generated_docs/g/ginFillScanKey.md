# ginFillScanKey

## Location
[src/backend/access/gin/ginscan.c:158-237](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginscan.c#L158-L237)

## Overview
Initializes a GinScanKey structure using output from the extractQueryFn, setting up scan entries and configuring search behavior based on the search mode.

## Definition

```c
static void
ginFillScanKey(GinScanOpaque so, OffsetNumber attnum,
			   StrategyNumber strategy, int32 searchMode,
			   Datum query, uint32 nQueryValues,
			   Datum *queryValues, GinNullCategory *queryCategories,
			   bool *partial_matches, Pointer *extra_data)
```
## Detailed Description
The  function processes the output from the opclass's extractQueryFn and creates a complete GinScanKey structure. It allocates arrays for scan entries and result values, creates individual scan entries for each query value using , and configures the key's behavior based on the search mode.

The function handles different search modes specially: GIN_SEARCH_MODE_ALL keys are initially marked as excludeOnly, GIN_SEARCH_MODE_INCLUDE_EMPTY adds a hidden entry for empty items, and GIN_SEARCH_MODE_EVERYTHING adds a hidden entry for empty queries. The function allocates one extra slot in the scan entry array to accommodate potential hidden entries.

## Parameters
- : GIN scan opaque data structure containing scan state
- : Attribute number being scanned
- : Strategy number for the search operator
- : Search mode flags (ALL, INCLUDE_EMPTY, EVERYTHING)
- : Original query datum
- : Number of extracted query values
- : Array of extracted query values
- : Array of categories for each query value
- : Array indicating which values are partial matches
- : Array of opclass-specific extra data

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md)
  - [palloc0](../p/palloc0.md)
  - ItemPointerSetMin
  - [ginInitConsistentFunction](ginInitConsistentFunction.md)
  - [ginFillScanEntry](ginFillScanEntry.md)
  - [ginScanKeyAddHiddenEntry](ginScanKeyAddHiddenEntry.md)
- Called from:
  - [ginNewScanKey](ginNewScanKey.md) (in multiple contexts)

## Notes and Other Information
- Allocates one extra slot in scan entry arrays for potential hidden entries
- Initially marks GIN_SEARCH_MODE_ALL keys as excludeOnly (may change later)
- Handles partial matches only if the opclass supports them for the attribute
- Automatically adds hidden entries for INCLUDE_EMPTY and EVERYTHING search modes
- Initializes the consistent function for the scan key
- Part of the query preprocessing phase in GIN index scanning

## Simplified Source

```c
static void ginFillScanKey(GinScanOpaque so, OffsetNumber attnum,
                         StrategyNumber strategy, int32 searchMode,
                         Datum query, uint32 nQueryValues,
                         Datum *queryValues, GinNullCategory *queryCategories,
                         bool *partial_matches, Pointer *extra_data) {
    GinScanKey key = &(so->keys[so->nkeys++]);
    GinState *ginstate = &so->ginstate;
    uint32 i;

    // Initialize key with extracted query information
    key->nentries = nQueryValues;
    key->nuserentries = nQueryValues;

    // Allocate arrays with one extra slot for potential hidden entry
    key->scanEntry = (GinScanEntry *) palloc(sizeof(GinScanEntry) * (nQueryValues + 1));
    key->entryRes = (GinTernaryValue *) palloc0(sizeof(GinTernaryValue) * (nQueryValues + 1));

    // Store query information
    key->query = query;
    key->queryValues = queryValues;
    key->queryCategories = queryCategories;
    key->extra_data = extra_data;
    key->strategy = strategy;
    key->searchMode = searchMode;
    key->attnum = attnum;

    // Initialize search behavior
    key->excludeOnly = (searchMode == GIN_SEARCH_MODE_ALL);
    ItemPointerSetMin(&key->curItem);
    key->curItemMatches = false;
    key->recheckCurItem = false;
    key->isFinished = false;
    key->nrequired = 0;
    key->nadditional = 0;
    key->requiredEntries = NULL;
    key->additionalEntries = NULL;

    ginInitConsistentFunction(ginstate, key);

    // Create scan entries for each extracted query value
    for (i = 0; i < nQueryValues; i++) {
        Datum queryKey = queryValues[i];
        GinNullCategory queryCategory = queryCategories[i];
        bool isPartialMatch = (ginstate->canPartialMatch[attnum - 1] && partial_matches)
                            ? partial_matches[i] : false;
        Pointer this_extra = (extra_data) ? extra_data[i] : NULL;

        key->scanEntry[i] = ginFillScanEntry(so, attnum, strategy, searchMode,
                                           queryKey, queryCategory,
                                           isPartialMatch, this_extra);
    }

    // Add hidden entries for specific search modes
    if (searchMode == GIN_SEARCH_MODE_INCLUDE_EMPTY)
        ginScanKeyAddHiddenEntry(so, key, GIN_CAT_EMPTY_ITEM);
    else if (searchMode == GIN_SEARCH_MODE_EVERYTHING)
        ginScanKeyAddHiddenEntry(so, key, GIN_CAT_EMPTY_QUERY);
}
```