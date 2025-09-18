# _bt_readpage

## Location
src/backend/access/nbtree/nbtsearch.c: 1560 - 1944

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