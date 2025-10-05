# PageIndexMultiDelete

## Location
[src/backend/storage/page/bufpage.c:1161-1294](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/page/bufpage.c#L1161-L1294)

## Overview
Efficiently deletes multiple tuples from an index page simultaneously, significantly faster than multiple individual deletions when processing more than 2 items.

## Definition

```c
void
PageIndexMultiDelete(Page page, OffsetNumber *itemnos, int nitems)
```
## Detailed Description
PageIndexMultiDelete is an optimized function for removing multiple tuples from an index page at once. It performs bulk deletion by rebuilding the line pointer array without the deleted items and then compacting the remaining tuple data. The function includes extensive validation checks and uses different strategies based on the number of items to delete:

- For 2 or fewer items: delegates to individual PageIndexTupleDelete calls in reverse order
- For more items: performs bulk processing by scanning line pointers, building a new array excluding deleted items, and compacting the remaining data

The function requires that the item numbers array be provided in sorted order and performs comprehensive corruption checks on page structure before making any modifications.

## Parameters / Member Variables
- `page`: The index page from which to delete tuples
- `*itemnos`: Array of item offset numbers to delete, must be in sorted order
- `nitems`: Number of items in the itemnos array, must be ≤ MaxIndexTuplesPerPage
## Dependencies
- Functions called/Symbols referenced:
  - [PageIndexTupleDelete](PageIndexTupleDelete.md)
  - [PageGetMaxOffsetNumber](PageGetMaxOffsetNumber.md)
  - [PageGetItemId](PageGetItemId.md)
  - ItemIdHasStorage
  - ItemIdGetLength
  - ItemIdGetOffset
  - [compactify_tuples](../c/compactify_tuples.md)
- Called from (representative examples):
  - [_bt_delitems_vacuum](../b/_bt_delitems_vacuum.md) (B-tree vacuum operations)
  - [_hash_vacuum_one_page](../h/_hash_vacuum_one_page.md) (Hash index vacuum)
  - [gistprunepage](../g/gistprunepage.md) (GiST index page pruning)
  - [spgPageIndexMultiDelete](../s/spgPageIndexMultiDelete.md) (SP-GiST operations)

## Notes and Other Information
- Critical requirement: item numbers must be provided in ascending order
- Includes magic number threshold (currently 2) below which individual deletions are preferred
- Performs extensive page corruption validation before modification
- Uses temporary arrays (itemidbase, newitemids) to build new page structure before committing changes
- Optimizes for presorted data during tuple compaction
- Essential for efficient bulk deletion operations in various index types (B-tree, Hash, GiST, SP-GiST)

## Simplified Source

```c
void PageIndexMultiDelete(Page page, OffsetNumber *itemnos, int nitems) {
    PageHeader phdr = (PageHeader) page;

    // For small deletions, use individual deletions in reverse order
    if (nitems <= 2) {
        while (--nitems >= 0) {
            PageIndexTupleDelete(page, itemnos[nitems]);
        }
        return;
    }

    // Validate page structure
    if (phdr->pd_lower < SizeOfPageHeaderData ||
        phdr->pd_lower > phdr->pd_upper ||
        phdr->pd_upper > phdr->pd_special) {
        ereport(ERROR, "corrupted page pointers");
    }

    // Build arrays for items to keep
    itemIdCompactData itemidbase[MaxIndexTuplesPerPage];
    ItemIdData newitemids[MaxIndexTuplesPerPage];

    int nline = PageGetMaxOffsetNumber(page);
    int nused = 0; // Count of items we're keeping
    int nextitm = 0; // Index into deletion array
    Size totallen = 0;
    bool presorted = true;
    Offset last_offset = phdr->pd_special;

    // Scan all line pointers and build list of ones to keep
    for (OffsetNumber offnum = FirstOffsetNumber; offnum <= nline; offnum++) {
        ItemId lp = PageGetItemId(page, offnum);
        Size size = ItemIdGetLength(lp);
        unsigned offset = ItemIdGetOffset(lp);

        // Check if this item should be deleted
        if (nextitm < nitems && offnum == itemnos[nextitm]) {
            nextitm++; // Skip this item - it's being deleted
        } else {
            // Keep this item - add to new arrays
            itemidbase[nused].offsetindex = nused;
            itemidbase[nused].itemoff = offset;
            itemidbase[nused].alignedlen = MAXALIGN(size);

            // Track sorting order for optimization
            if (last_offset > offset) {
                last_offset = offset;
            } else {
                presorted = false;
            }

            totallen += itemidbase[nused].alignedlen;
            newitemids[nused] = *lp;
            nused++;
        }
    }

    // Verify all deletions were found
    if (nextitm != nitems) {
        elog(ERROR, "incorrect index offsets supplied");
    }

    // Replace line pointer array with compacted version
    memcpy(phdr->pd_linp, newitemids, nused * sizeof(ItemIdData));
    phdr->pd_lower = SizeOfPageHeaderData + nused * sizeof(ItemIdData);

    // Compact the tuple data
    if (nused > 0) {
        compactify_tuples(itemidbase, nused, page, presorted);
    } else {
        phdr->pd_upper = phdr->pd_special; // Empty page
    }
}
```