# _bt_readpage

## Location
[src/backend/access/nbtree/nbtsearch.c:1560-1944](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsearch.c#L1560-L1944)

## Overview
Loads qualifying data from the current index page into the scan position structure, filtering tuples based on scan keys and handling both regular and posting list tuples.

## Definition
```c
static bool _bt_readpage(IndexScanDesc scan, ScanDirection dir, OffsetNumber offnum, bool firstPage)
```

## Detailed Description
This function is responsible for scanning a B-tree leaf page and loading all qualifying tuples into the scan's current position structure (so->currPos). It handles the complex logic of evaluating scan keys against each tuple on the page, managing both forward and backward scan directions, and properly processing posting list tuples that contain multiple heap TIDs.

The function implements several optimizations including precheck logic to avoid redundant key evaluations, early termination when no more matches are possible, and efficient handling of array keys. It also manages parallel scan coordination and handles killed tuple filtering when requested.

## Parameters / Member Variables
- `scan`: IndexScanDesc containing the scan state and qualification criteria
- `dir`: ScanDirection indicating forward or backward scan direction
- `offnum`: Starting offset number on the page (returned by _bt_binsrch)
- `firstPage`: Boolean indicating if this is the first page being read in the scan

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](../B/BufferGetPage.md)
  - BTPageGetOpaque
  - [_bt_parallel_release](_bt_parallel_release.md)
  - IndexRelationGetNumberOfAttributes
  - P_FIRSTDATAKEY
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)
  - [BufferGetLSNAtomic](../B/BufferGetLSNAtomic.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - [PageGetItem](../P/PageGetItem.md)
  - [_bt_checkkeys](_bt_checkkeys.md)
  - ItemIdIsDead
  - [BTreeTupleIsPivot](../B/BTreeTupleIsPivot.md)
  - [BTreeTupleIsPosting](../B/BTreeTupleIsPosting.md)
  - [_bt_saveitem](_bt_saveitem.md)
  - [_bt_setuppostingitems](_bt_setuppostingitems.md)
  - [_bt_savepostingitem](_bt_savepostingitem.md)
  - [BTreeTupleGetPostingN](../B/BTreeTupleGetPostingN.md)
  - [BTreeTupleGetNPosting](../B/BTreeTupleGetNPosting.md)
  - BTreeTupleGetNAtts
- Called from:
  - [_bt_first](_bt_first.md)
  - [_bt_readnextpage](_bt_readnextpage.md)
  - [_bt_endpoint](_bt_endpoint.md)

## Notes and Other Information
- Returns true if any matching items were found on the page, false if none
- Handles both forward and backward scan directions with different item loading strategies
- Implements precheck optimization to avoid redundant key evaluations across all page items
- Properly processes posting list tuples by expanding them into individual TID entries
- Manages parallel scan state and releases coordination locks appropriately
- Handles killed tuple filtering based on scan->ignore_killed_tuples setting
- Sets moreLeft/moreRight flags to indicate whether more matches exist in respective directions
- Critical for B-tree scan performance as it determines which tuples qualify for return
- Implements sophisticated array key handling with skip-ahead optimization
- Maintains proper tuple ordering in the items array for both scan directions
- Essential component of PostgreSQL's B-tree access method implementation

## Simplified Source

```c
static bool
_bt_readpage(IndexScanDesc scan, ScanDirection dir, OffsetNumber offnum, bool firstPage)
{
    BTScanOpaque so = (BTScanOpaque) scan->opaque;
    Page page;
    BTPageOpaque opaque;
    OffsetNumber minoff, maxoff;
    BTReadPageState pstate;
    int itemIndex, indnatts;

    page = BufferGetPage(so->currPos.buf);
    opaque = BTPageGetOpaque(page);

    // Handle parallel scan coordination
    if (scan->parallel_scan)
    {
        BlockNumber next_page = ScanDirectionIsForward(dir) ?
                               opaque->btpo_next :
                               BufferGetBlockNumber(so->currPos.buf);
        _bt_parallel_release(scan, next_page);
    }

    // Initialize page scanning state
    indnatts = IndexRelationGetNumberOfAttributes(scan->indexRelation);
    minoff = P_FIRSTDATAKEY(opaque);
    maxoff = PageGetMaxOffsetNumber(page);

    pstate.dir = dir;
    pstate.continuescan = true;
    pstate.prechecked = false;

    // Save page information for scan state
    so->currPos.currPage = BufferGetBlockNumber(so->currPos.buf);
    so->currPos.lsn = BufferGetLSNAtomic(so->currPos.buf);
    so->currPos.nextPage = opaque->btpo_next;
    so->currPos.dir = dir;
    so->currPos.nextTupleOffset = 0;

    // Perform precheck optimization on representative tuple
    if (!firstPage && !so->scanBehind && minoff < maxoff)
    {
        ItemId iid = PageGetItemId(page, ScanDirectionIsForward(dir) ? maxoff : minoff);
        IndexTuple itup = (IndexTuple) PageGetItem(page, iid);
        _bt_checkkeys(scan, &pstate, false, itup, indnatts);
        pstate.prechecked = pstate.continuescan;
        pstate.continuescan = true;
    }

    if (ScanDirectionIsForward(dir))
    {
        // Forward scan: load items in ascending order
        itemIndex = 0;
        offnum = Max(offnum, minoff);

        while (offnum <= maxoff)
        {
            ItemId iid = PageGetItemId(page, offnum);
            IndexTuple itup;

            // Skip killed tuples if requested
            if (scan->ignore_killed_tuples && ItemIdIsDead(iid))
            {
                offnum = OffsetNumberNext(offnum);
                continue;
            }

            itup = (IndexTuple) PageGetItem(page, iid);
            pstate.offnum = offnum;

            // Check if tuple qualifies
            if (_bt_checkkeys(scan, &pstate, (so->numArrayKeys != 0), itup, indnatts))
            {
                // Save qualifying tuple(s)
                if (!BTreeTupleIsPosting(itup))
                {
                    _bt_saveitem(so, itemIndex, offnum, itup);
                    itemIndex++;
                }
                else
                {
                    // Handle posting list tuple
                    int tupleOffset = _bt_setuppostingitems(so, itemIndex, offnum,
                                                          BTreeTupleGetPostingN(itup, 0), itup);
                    itemIndex++;
                    // Save additional TIDs from posting list
                    for (int i = 1; i < BTreeTupleGetNPosting(itup); i++)
                    {
                        _bt_savepostingitem(so, itemIndex, offnum,
                                          BTreeTupleGetPostingN(itup, i), tupleOffset);
                        itemIndex++;
                    }
                }
            }

            if (!pstate.continuescan)
                break;

            offnum = OffsetNumberNext(offnum);
        }

        // Check high key to determine if more pages needed
        if (pstate.continuescan && !P_RIGHTMOST(opaque))
        {
            ItemId iid = PageGetItemId(page, P_HIKEY);
            IndexTuple itup = (IndexTuple) PageGetItem(page, iid);
            int truncatt = BTreeTupleGetNAtts(itup, scan->indexRelation);
            _bt_checkkeys(scan, &pstate, (so->numArrayKeys != 0), itup, truncatt);
        }

        if (!pstate.continuescan)
            so->currPos.moreRight = false;

        so->currPos.firstItem = 0;
        so->currPos.lastItem = itemIndex - 1;
        so->currPos.itemIndex = 0;
    }
    else
    {
        // Backward scan: load items in descending order
        itemIndex = MaxTIDsPerBTreePage;
        offnum = Min(offnum, maxoff);

        while (offnum >= minoff)
        {
            ItemId iid = PageGetItemId(page, offnum);
            IndexTuple itup;
            bool tuple_alive = true;

            // Handle killed tuples for backward scan
            if (scan->ignore_killed_tuples && ItemIdIsDead(iid))
            {
                if (offnum > P_FIRSTDATAKEY(opaque))
                {
                    offnum = OffsetNumberPrev(offnum);
                    continue;
                }
                tuple_alive = false;
            }

            itup = (IndexTuple) PageGetItem(page, iid);
            pstate.offnum = offnum;

            // Check if tuple qualifies
            if (_bt_checkkeys(scan, &pstate, (so->numArrayKeys != 0), itup, indnatts) && tuple_alive)
            {
                // Save qualifying tuple(s) in reverse order
                if (!BTreeTupleIsPosting(itup))
                {
                    itemIndex--;
                    _bt_saveitem(so, itemIndex, offnum, itup);
                }
                else
                {
                    // Handle posting list tuple
                    itemIndex--;
                    int tupleOffset = _bt_setuppostingitems(so, itemIndex, offnum,
                                                          BTreeTupleGetPostingN(itup, 0), itup);
                    // Save additional TIDs from posting list
                    for (int i = 1; i < BTreeTupleGetNPosting(itup); i++)
                    {
                        itemIndex--;
                        _bt_savepostingitem(so, itemIndex, offnum,
                                          BTreeTupleGetPostingN(itup, i), tupleOffset);
                    }
                }
            }

            if (!pstate.continuescan)
            {
                so->currPos.moreLeft = false;
                break;
            }

            offnum = OffsetNumberPrev(offnum);
        }

        so->currPos.firstItem = itemIndex;
        so->currPos.lastItem = MaxTIDsPerBTreePage - 1;
        so->currPos.itemIndex = MaxTIDsPerBTreePage - 1;
    }

    return (so->currPos.firstItem <= so->currPos.lastItem);
}
```