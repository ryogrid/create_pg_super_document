# matchPartialInPendingList

## Location
[src/backend/access/gin/ginget.c:1541-1608](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginget.c#L1541-L1608)

## Overview
Scans a pending list page to find partial matches for a given scan entry, using cached datum extraction and the comparePartial function.

## Definition
```c
static bool matchPartialInPendingList(GinState *ginstate, Page page, OffsetNumber off, OffsetNumber maxoff, GinScanEntry entry, Datum *datum, GinNullCategory *category, bool *datumExtracted)
```

## Detailed Description
This function performs partial matching within a single pending list page by scanning tuples from a starting offset until finding a match, encountering a non-matching condition, reaching attribute boundary, or hitting page end. It optimizes performance by caching extracted datums in provided arrays to avoid redundant key extraction operations.

The function specifically handles partial match semantics where the comparison can return three states: exact match (0), no match but continue scanning (<0), or no match and stop scanning (>0). It only processes normal key categories and stops immediately when encountering null items or different attribute numbers.

## Parameters / Member Variables
- `ginstate`: GIN state containing operation context and function pointers
- `page`: Page containing the pending list tuples to scan
- `off`: Starting offset number for the scan
- `maxoff`: Maximum offset number (exclusive end of scan range)
- `entry`: Scan entry containing query information and partial match criteria
- `datum`: Array for caching extracted datum values to avoid re-extraction
- `category`: Array for caching null category information
- `datumExtracted`: Boolean array tracking which positions have cached data

## Dependencies
- Functions called/Symbols referenced:
  - [PageGetItem](../P/PageGetItem.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - [gintuple_get_attrnum](../g/gintuple_get_attrnum.md)
  - [gintuple_get_key](../g/gintuple_get_key.md)
  - [FunctionCall4Coll](../F/FunctionCall4Coll.md)
  - [DatumGetInt32](../D/DatumGetInt32.md)
  - [UInt16GetDatum](../U/UInt16GetDatum.md)
- Called from (representative examples):
  - [collectMatchesForHeapRow](../c/collectMatchesForHeapRow.md)

## Notes and Other Information
Critical optimization for partial match queries in GIN indexes. The caching mechanism significantly improves performance when the same page is scanned multiple times for different entries. The function assumes tuples are ordered by attribute number and datum value, allowing early termination when attribute boundaries are crossed.

## Simplified Source
```c
static bool matchPartialInPendingList(GinState *ginstate, Page page,
                                     OffsetNumber off, OffsetNumber maxoff,
                                     GinScanEntry entry,
                                     Datum *datum, GinNullCategory *category,
                                     bool *datumExtracted) {
    IndexTuple itup;
    int32 cmp;

    // Partial match to null is not possible
    if (entry->queryCategory != GIN_CAT_NORM_KEY)
        return false;

    while (off < maxoff) {
        itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, off));

        // Stop if we've moved to a different attribute
        if (gintuple_get_attrnum(ginstate, itup) != entry->attnum)
            return false;

        // Extract datum if not already cached
        if (datumExtracted[off - 1] == false) {
            datum[off - 1] = gintuple_get_key(ginstate, itup, &category[off - 1]);
            datumExtracted[off - 1] = true;
        }

        // Stop at nulls - no further matches possible
        if (category[off - 1] != GIN_CAT_NORM_KEY)
            return false;

        // Perform partial match comparison
        cmp = DatumGetInt32(FunctionCall4Coll(&ginstate->comparePartialFn[entry->attnum - 1],
                                             ginstate->supportCollation[entry->attnum - 1],
                                             entry->queryKey,
                                             datum[off - 1],
                                             UInt16GetDatum(entry->strategy),
                                             PointerGetDatum(entry->extra_data)));

        if (cmp == 0)
            return true;    // Match found
        else if (cmp > 0)
            return false;   // No match and no later match possible

        // cmp < 0: no match but continue scanning
        off++;
    }

    return false;
}
```