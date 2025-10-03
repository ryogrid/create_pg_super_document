# ginFillScanEntry

## Location
[src/backend/access/gin/ginscan.c:57-141](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginscan.c#L57-L141)

## Overview
Creates a new GinScanEntry for a GIN index scan, or returns an existing equivalent entry to avoid duplication and optimize performance.

## Definition

```c
static GinScanEntry
ginFillScanEntry(GinScanOpaque so, OffsetNumber attnum,
				 StrategyNumber strategy, int32 searchMode,
				 Datum queryKey, GinNullCategory queryCategory,
				 bool isPartialMatch, Pointer extra_data)
```
## Detailed Description
The  function creates and initializes a new GinScanEntry structure that represents a single search condition in a GIN index scan. Before creating a new entry, it attempts to find an existing equivalent entry to avoid duplication, which can significantly improve performance for complex queries with overlapping conditions.

The function implements a deduplication strategy with two important limitations: entries with extra_data are never considered identical (since opclass behavior with extra_data is unpredictable), and deduplication is limited to the first 100 entries to avoid O(N²) performance degradation on large search-key sets.

When creating a new entry, the function initializes all scan-related fields to their starting values and adds the entry to the scan's entries array, expanding the array if necessary.

## Parameters
- : GIN scan opaque data structure containing scan state
- : Attribute number being searched
- : Strategy number for the search operator
- : Search mode flags for the operation
- : The key value being searched for
- : Category of the query key (regular, null, etc.)
- : Whether this is a partial match operation
- : Additional opclass-specific data (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [ginCompareEntries](ginCompareEntries.md)
  - [palloc](../p/palloc.md)
  - ItemPointerSetMin
  - [repalloc](../r/repalloc.md)
- Called from:
  - [ginScanKeyAddHiddenEntry](ginScanKeyAddHiddenEntry.md)
  - [ginFillScanKey](ginFillScanKey.md)

## Notes and Other Information
- Implements deduplication to avoid creating duplicate scan entries for identical search conditions
- Deduplication is limited to 100 entries to prevent O(N²) performance issues
- Entries with extra_data are never deduplicated due to unpredictable opclass behavior
- Initializes all scan state fields to starting values (buffer=InvalidBuffer, isFinished=false, etc.)
- Dynamically expands the entries array using repalloc when needed

## Simplified Source

```c
static GinScanEntry ginFillScanEntry(GinScanOpaque so, OffsetNumber attnum,
                                   StrategyNumber strategy, int32 searchMode,
                                   Datum queryKey, GinNullCategory queryCategory,
                                   bool isPartialMatch, Pointer extra_data) {
    GinState *ginstate = &so->ginstate;
    GinScanEntry scanEntry;
    uint32 i;

    // Try to find existing equivalent entry for deduplication
    // Skip if extra_data present or too many entries (avoid O(N²) cost)
    if (extra_data == NULL && so->totalentries < 100) {
        for (i = 0; i < so->totalentries; i++) {
            GinScanEntry prevEntry = so->entries[i];

            // Check if entries match on all relevant fields
            if (prevEntry->extra_data == NULL &&
                prevEntry->isPartialMatch == isPartialMatch &&
                prevEntry->strategy == strategy &&
                prevEntry->searchMode == searchMode &&
                prevEntry->attnum == attnum &&
                ginCompareEntries(ginstate, attnum,
                                prevEntry->queryKey, prevEntry->queryCategory,
                                queryKey, queryCategory) == 0) {
                return prevEntry; // Found equivalent entry
            }
        }
    }

    // Create new entry since no equivalent found
    scanEntry = (GinScanEntry) palloc(sizeof(GinScanEntryData));
    scanEntry->queryKey = queryKey;
    scanEntry->queryCategory = queryCategory;
    scanEntry->isPartialMatch = isPartialMatch;
    scanEntry->extra_data = extra_data;
    scanEntry->strategy = strategy;
    scanEntry->searchMode = searchMode;
    scanEntry->attnum = attnum;

    // Initialize scan state fields
    scanEntry->buffer = InvalidBuffer;
    ItemPointerSetMin(&scanEntry->curItem);
    scanEntry->matchBitmap = NULL;
    scanEntry->matchIterator = NULL;
    scanEntry->matchResult = NULL;
    scanEntry->list = NULL;
    scanEntry->nlist = 0;
    scanEntry->offset = InvalidOffsetNumber;
    scanEntry->isFinished = false;
    scanEntry->reduceResult = false;

    // Add to scan's entries array, expanding if necessary
    if (so->totalentries >= so->allocentries) {
        so->allocentries *= 2;
        so->entries = (GinScanEntry *) repalloc(so->entries,
                                               so->allocentries * sizeof(GinScanEntry));
    }
    so->entries[so->totalentries++] = scanEntry;

    return scanEntry;
}
```