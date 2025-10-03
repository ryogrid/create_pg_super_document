# ginVacuumEntryPage

## Location
[src/backend/access/gin/ginvacuum.c:456-564](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginvacuum.c#L456-L564)

## Overview
A static function that processes entry pages during GIN index vacuum operations, removing dead tuples from posting lists and collecting posting tree roots for later processing.

## Definition
```c
static Page ginVacuumEntryPage(GinVacuumState *gvs, Buffer buffer, BlockNumber *roots, uint32 *nroot)
```

## Detailed Description
This function performs vacuum operations on GIN entry pages by examining each index tuple and processing posting lists. It handles two types of tuples: those with posting trees (which are deferred for later processing) and those with posting lists (which are processed immediately). For posting lists, it removes dead item pointers and reconstructs the tuple if necessary.

The function uses a copy-on-write strategy where it works with the original page until the first modification is needed, then creates a temporary copy. This optimization avoids unnecessary copying when no changes are required. For compressed posting lists, it decompresses them, removes dead items, and recompresses them back into the tuple.

## Parameters / Member Variables
- `gvs`: Pointer to GinVacuumState structure containing vacuum context and dead tuple information
- `buffer`: Buffer containing the entry page to be processed
- `roots`: Output array to store block numbers of posting tree roots found on this page
- `nroot`: Output parameter indicating the number of posting tree roots found

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](../B/BufferGetPage.md)
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - [PageGetItem](../P/PageGetItem.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - GinIsPostingTree
  - GinGetDownlink
  - GinGetNPosting
  - GinItupIsCompressed
  - [ginPostingListDecode](ginPostingListDecode.md)
  - [ginVacuumItemPointers](ginVacuumItemPointers.md)
  - [ginCompressPostingList](ginCompressPostingList.md)
  - [PageGetTempPageCopy](../P/PageGetTempPageCopy.md)
  - [GinFormTuple](../G/GinFormTuple.md)
  - [PageIndexTupleDelete](../P/PageIndexTupleDelete.md)
  - PageAddItem
- Called from (representative examples):
  - [ginbulkdelete](ginbulkdelete.md)

## Notes and Other Information
- Returns modified page or NULL if no modifications were made
- Uses copy-on-write optimization to avoid unnecessary page copying
- Posting tree roots are collected but not processed immediately to avoid deadlock risks
- Handles both compressed and uncompressed posting lists
- Properly manages memory allocation and deallocation for temporary data structures
- Part of the GIN index vacuum system that maintains posting list integrity
- The function maintains the original page structure while selectively updating individual tuples

## Simplified Source

```c
static Page
ginVacuumEntryPage(GinVacuumState *gvs, Buffer buffer, BlockNumber *roots, uint32 *nroot)
{
    Page origpage = BufferGetPage(buffer), tmppage;
    OffsetNumber i, maxoff = PageGetMaxOffsetNumber(origpage);

    tmppage = origpage;  // Use copy-on-write strategy
    *nroot = 0;

    // Process each tuple on the page
    for (i = FirstOffsetNumber; i <= maxoff; i++) {
        IndexTuple itup = (IndexTuple) PageGetItem(tmppage, PageGetItemId(tmppage, i));

        if (GinIsPostingTree(itup)) {
            // Collect posting tree roots for later processing
            roots[*nroot] = GinGetDownlink(itup);
            (*nroot)++;
        } else if (GinGetNPosting(itup) > 0) {
            // Process posting list tuples
            int nitems;
            ItemPointer items_orig, items;
            bool free_items_orig;

            // Extract item pointers from tuple (compressed or uncompressed)
            if (GinItupIsCompressed(itup)) {
                items_orig = ginPostingListDecode((GinPostingList *) GinGetPosting(itup), &nitems);
                free_items_orig = true;
            } else {
                items_orig = (ItemPointer) GinGetPosting(itup);
                nitems = GinGetNPosting(itup);
                free_items_orig = false;
            }

            // Remove dead item pointers
            items = ginVacuumItemPointers(gvs, items_orig, nitems, &nitems);

            if (free_items_orig)
                pfree(items_orig);

            // If items were removed, recreate the tuple
            if (items) {
                OffsetNumber attnum;
                Datum key;
                GinNullCategory category;
                GinPostingList *plist;
                int plistsize;

                // Compress posting list if items remain
                if (nitems > 0) {
                    plist = ginCompressPostingList(items, nitems, GinMaxItemSize, NULL);
                    plistsize = SizeOfGinPostingList(plist);
                } else {
                    plist = NULL;
                    plistsize = 0;
                }

                // Create temporary page copy on first modification
                if (tmppage == origpage) {
                    tmppage = PageGetTempPageCopy(origpage);
                    itup = (IndexTuple) PageGetItem(tmppage, PageGetItemId(tmppage, i));
                }

                // Recreate tuple with updated posting list
                attnum = gintuple_get_attrnum(&gvs->ginstate, itup);
                key = gintuple_get_key(&gvs->ginstate, itup, &category);
                itup = GinFormTuple(&gvs->ginstate, attnum, key, category,
                                   (char *) plist, plistsize, nitems, true);

                // Replace tuple on page
                PageIndexTupleDelete(tmppage, i);
                if (PageAddItem(tmppage, (Item) itup, IndexTupleSize(itup), i, false, false) != i)
                    elog(ERROR, "failed to add item to index page in \"%s\"",
                         RelationGetRelationName(gvs->index));

                // Clean up
                if (plist)
                    pfree(plist);
                pfree(itup);
                pfree(items);
            }
        }
    }

    return (tmppage == origpage) ? NULL : tmppage;
}
```