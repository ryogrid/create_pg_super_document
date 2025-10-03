# collectMatchesForHeapRow

## Location
[src/backend/access/gin/ginget.c:1609-1823](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginget.c#L1609-L1823)

## Overview
Examines all pending list entries for a current heap row and populates the entryRes array for each scan key, determining if the row satisfies search criteria.

## Definition
```c
static bool collectMatchesForHeapRow(IndexScanDesc scan, pendingPosition *pos)
```

## Detailed Description
This function processes all pending entries belonging to a single heap row across potentially multiple pages, building a complete picture of which scan entry conditions are satisfied. It uses binary search optimization to efficiently locate matching entries within the ordered pending list structure, taking advantage of the (attnum, Datum) ordering.

The function handles both exact matches and partial matches, with special logic for EMPTY_QUERY entries that have different matching semantics. For heap rows spanning multiple pages, it coordinates with scanGetCandidate to process all relevant pages while maintaining proper position tracking.

Performance is optimized through datum caching arrays that prevent redundant key extraction operations when the same tuple is examined by multiple scan entries. The function returns true only when all non-excludeOnly scan keys have at least one matching entry.

## Parameters / Member Variables
- `scan`: Index scan descriptor containing scan keys and state information
- `pos`: Pending position structure defining the heap row's tuple range and managing page transitions

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](../B/BufferGetPage.md)
  - [gintuple_get_attrnum](../g/gintuple_get_attrnum.md)
  - [gintuple_get_key](../g/gintuple_get_key.md)
  - [ginCompareEntries](../g/ginCompareEntries.md)
  - [matchPartialInPendingList](../m/matchPartialInPendingList.md)
  - GinPageHasFullRow
  - [scanGetCandidate](../s/scanGetCandidate.md)
  - [ItemPointerEquals](../I/ItemPointerEquals.md)
- Called from (representative examples):
  - [scanPendingInsert](../s/scanPendingInsert.md)

## Notes and Other Information
Central component of pending list processing that determines whether heap rows satisfy query conditions. The binary search optimization is crucial for performance on large pending lists. The function must correctly handle the transition between pages when a single heap row's entries span multiple pending list pages, which can occur during high-volume insert scenarios.

## Simplified Source
```c
static bool collectMatchesForHeapRow(IndexScanDesc scan, pendingPosition *pos) {
    GinScanOpaque so = (GinScanOpaque) scan->opaque;
    Page page;
    IndexTuple itup;
    OffsetNumber attrnum;
    int i, j;

    // Reset all entryRes and hasMatchKey flags
    for (i = 0; i < so->nkeys; i++) {
        GinScanKey key = so->keys + i;
        memset(key->entryRes, GIN_FALSE, key->nentries);
    }
    memset(pos->hasMatchKey, false, so->nkeys);

    // Process all pages containing entries for this heap row
    for (;;) {
        // Cache arrays for datum extraction optimization
        Datum datum[BLCKSZ / sizeof(IndexTupleData)];
        GinNullCategory category[BLCKSZ / sizeof(IndexTupleData)];
        bool datumExtracted[BLCKSZ / sizeof(IndexTupleData)];

        memset(datumExtracted + pos->firstOffset - 1, 0,
               sizeof(bool) * (pos->lastOffset - pos->firstOffset));

        page = BufferGetPage(pos->pendingBuffer);

        // Check each scan key against pending entries
        for (i = 0; i < so->nkeys; i++) {
            GinScanKey key = so->keys + i;

            for (j = 0; j < key->nentries; j++) {
                GinScanEntry entry = key->scanEntry[j];
                OffsetNumber StopLow = pos->firstOffset;
                OffsetNumber StopHigh = pos->lastOffset;
                OffsetNumber StopMiddle;

                if (key->entryRes[j])
                    continue;  // Already matched on earlier page

                // Binary search for matching entries
                while (StopLow < StopHigh) {
                    int res;

                    StopMiddle = StopLow + ((StopHigh - StopLow) >> 1);
                    itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, StopMiddle));
                    attrnum = gintuple_get_attrnum(&so->ginstate, itup);

                    // Navigate binary search by attribute number
                    if (key->attnum < attrnum) {
                        StopHigh = StopMiddle;
                        continue;
                    }
                    if (key->attnum > attrnum) {
                        StopLow = StopMiddle + 1;
                        continue;
                    }

                    // Extract datum if not cached
                    if (datumExtracted[StopMiddle - 1] == false) {
                        datum[StopMiddle - 1] = gintuple_get_key(&so->ginstate, itup,
                                                               &category[StopMiddle - 1]);
                        datumExtracted[StopMiddle - 1] = true;
                    }

                    // Compare entry with cached datum
                    if (entry->queryCategory == GIN_CAT_EMPTY_QUERY) {
                        // Special handling for empty queries
                        if (entry->searchMode == GIN_SEARCH_MODE_ALL) {
                            res = (category[StopMiddle - 1] == GIN_CAT_NULL_ITEM) ? -1 : 0;
                        } else {
                            res = 0;  // Match everything
                        }
                    } else {
                        res = ginCompareEntries(&so->ginstate, entry->attnum,
                                              entry->queryKey, entry->queryCategory,
                                              datum[StopMiddle - 1], category[StopMiddle - 1]);
                    }

                    if (res == 0) {
                        // Found exact match - handle partial matching if needed
                        if (entry->isPartialMatch) {
                            key->entryRes[j] = matchPartialInPendingList(&so->ginstate, page,
                                                                       StopMiddle, pos->lastOffset,
                                                                       entry, datum, category,
                                                                       datumExtracted);
                        } else {
                            key->entryRes[j] = true;
                        }
                        break;
                    } else if (res < 0) {
                        StopHigh = StopMiddle;
                    } else {
                        StopLow = StopMiddle + 1;
                    }
                }

                // Handle partial match when no exact match found
                if (StopLow >= StopHigh && entry->isPartialMatch) {
                    key->entryRes[j] = matchPartialInPendingList(&so->ginstate, page,
                                                               StopHigh, pos->lastOffset,
                                                               entry, datum, category,
                                                               datumExtracted);
                }

                pos->hasMatchKey[i] |= key->entryRes[j];
            }
        }

        // Advance to next page if this heap row spans multiple pages
        pos->firstOffset = pos->lastOffset;

        if (GinPageHasFullRow(page)) {
            break;  // All entries for this heap row processed
        } else {
            // Move to next page for same heap row
            ItemPointerData item = pos->item;
            if (scanGetCandidate(scan, pos) == false ||
                !ItemPointerEquals(&pos->item, &item))
                elog(ERROR, "could not find additional pending pages for same heap tuple");
        }
    }

    // Check if all non-excludeOnly keys have matches
    for (i = 0; i < so->nkeys; i++) {
        if (pos->hasMatchKey[i] == false && !so->keys[i].excludeOnly)
            return false;
    }

    return true;
}
```