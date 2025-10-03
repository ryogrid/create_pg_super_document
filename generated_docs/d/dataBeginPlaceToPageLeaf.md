# dataBeginPlaceToPageLeaf

## Location
[src/backend/access/gin/gindatapage.c:448-715](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/gindatapage.c#L448-L715)

## Overview
dataBeginPlaceToPageLeaf prepares to insert data items into a GIN leaf data page, determining whether the items fit or if a page split is required.

## Definition
static GinPlaceToPageRC dataBeginPlaceToPageLeaf(GinBtree btree, Buffer buf, GinBtreeStack *stack, void *insertdata, void **ptp_workspace, Page *newlpage, Page *newrpage)

## Detailed Description
This function is the core logic for inserting posting list items into GIN leaf data pages. It performs sophisticated space management by first determining how many new items can fit on the current page based on page boundaries and available space. The function disassembles the existing compressed data, merges new items while removing duplicates, and recompresses the data. If the items fit, it prepares for in-place insertion. If not, it performs intelligent page splitting with different strategies for bulk loading versus normal operations, and implements append-optimized heuristics (75% left page fill for append patterns vs 50/50 for random inserts). The function also handles WAL logging preparation and provides detailed debugging information about the operation.

## Parameters / Member Variables
- btree: GinBtree structure containing B-tree context and configuration
- buf: Buffer containing the target leaf page to insert into
- stack: GinBtreeStack with navigation context for the insertion point
- insertdata: GinBtreeDataLeafInsertData containing the items to insert
- ptp_workspace: Workspace pointer for passing data to execution phase
- newlpage: Output parameter for left page image if splitting is required
- newrpage: Output parameter for right page image if splitting is required

## Dependencies
- Functions called/Symbols referenced:
  - GinDataPageGetRightBound
  - GinPageRightMost
  - [ginCompareItemPointers](../g/ginCompareItemPointers.md)
  - [disassembleLeaf](disassembleLeaf.md)
  - [ginPostingListDecode](../g/ginPostingListDecode.md)
  - GinPageIsCompressed
  - GinDataLeafPageGetFreeSpace
  - [addItemsToLeaf](../a/addItemsToLeaf.md)
  - [leafRepackItems](../l/leafRepackItems.md)
  - [computeLeafRecompressWALData](../c/computeLeafRecompressWALData.md)
  - [dataPlaceToPageLeafSplit](dataPlaceToPageLeafSplit.md)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)
- Called from (representative examples):
  - [dataBeginPlaceToPage](dataBeginPlaceToPage.md)

## Notes and Other Information
- Implements sophisticated space estimation to avoid failed insertions
- Uses different splitting strategies for bulk loading (fill left page) vs normal operations (balanced split)
- Optimizes for append patterns with 75% left page fill heuristic
- Handles compressed posting list segments with complex memory management
- Validates page boundaries to ensure items go to the correct page in the B-tree
- Supports incremental insertion by tracking current item position
- Returns different result codes: GPTP_INSERT (fits), GPTP_SPLIT (needs split), GPTP_NO_WORK (all duplicates)
- Part of the GIN (Generalized Inverted Index) access method implementation
- Critical for maintaining GIN index performance and storage efficiency

## Simplified Source

```c
static GinPlaceToPageRC dataBeginPlaceToPageLeaf(GinBtree btree, Buffer buf, GinBtreeStack *stack,
                                                void *insertdata, void **ptp_workspace,
                                                Page *newlpage, Page *newrpage) {
    GinBtreeDataLeafInsertData *items = insertdata;
    ItemPointer newItems = &items->items[items->curitem];
    int maxitems = items->nitem - items->curitem;
    Page page = BufferGetPage(buf);

    // Determine how many items belong on this page (respect page boundaries)
    ItemPointerData rbound = *GinDataPageGetRightBound(page);
    if (!GinPageRightMost(page)) {
        for (int i = 0; i < maxitems; i++) {
            if (ginCompareItemPointers(&newItems[i], &rbound) > 0) {
                maxitems = i; // Stop at page boundary
                break;
            }
        }
    }

    // Disassemble existing page data for processing
    disassembledLeaf *leaf = disassembleLeaf(page);

    // Determine if we're appending to end of page
    bool append = true;
    if (!dlist_is_empty(&leaf->segments)) {
        // Check if new items are larger than existing ones
        ItemPointerData maxOldItem = /* get max existing item */;
        append = (ginCompareItemPointers(&newItems[0], &maxOldItem) >= 0);
    }

    // Estimate space and limit items accordingly
    Size freespace = GinPageIsCompressed(page) ? GinDataLeafPageGetFreeSpace(page) : 0;
    if (append) {
        maxitems = Min(maxitems, freespace + GinDataPageMaxDataSize);
    } else {
        // Conservative estimate for non-append case
        int segments = freespace / GinPostingListSegmentMaxSize;
        segments += GinDataPageMaxDataSize / GinPostingListSegmentMaxSize;
        maxitems = Min(maxitems, segments * MinTuplesPerSegment);
    }

    // Add new items to the leaf structure
    if (!addItemsToLeaf(leaf, newItems, maxitems)) {
        items->curitem += maxitems;
        return GPTP_NO_WORK; // All duplicates
    }

    // Repack items and determine if split is needed
    ItemPointerData remaining;
    bool needsplit = leafRepackItems(leaf, &remaining);

    if (!needsplit) {
        // Items fit - prepare for single page update
        if (RelationNeedsWAL(btree->index) && !btree->isBuild) {
            computeLeafRecompressWALData(leaf);
        }
        *ptp_workspace = leaf;
    } else {
        // Split required - create left and right pages
        if (!btree->isBuild) {
            // Balance pages 50/50 or 75/25 for append case
            while (/* rebalancing condition */) {
                // Move segments from left to right page
            }
        }

        // Create new page images
        *newlpage = palloc(BLCKSZ);
        *newrpage = palloc(BLCKSZ);
        dataPlaceToPageLeafSplit(leaf, lbound, rbound, *newlpage, *newrpage);
    }

    items->curitem += maxitems;
    return needsplit ? GPTP_SPLIT : GPTP_INSERT;
}
```