# _bt_next

## Location
[src/backend/access/nbtree/nbtsearch.c:1496-1559](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsearch.c#L1496-L1559)

## Overview
Advances a B-tree scan to the next item, handling both forward and backward scan directions and managing page transitions when necessary.

## Definition
```c
bool _bt_next(IndexScanDesc scan, ScanDirection dir)
```

## Detailed Description
This function continues a B-tree scan that was previously initialized by _bt_first(). It advances the scan position to the next qualifying tuple in the specified direction. If there are more items on the current page, it simply advances the item index. When the current page is exhausted, it calls _bt_steppage() to move to the next page with qualifying data.

The function maintains the scan state in so->currPos and updates scan->xs_heaptid with the heap TID of the next tuple. It handles the mechanics of scanning both forward and backward through the B-tree structure while respecting page boundaries.

## Parameters / Member Variables
- `scan`: IndexScanDesc containing the current scan state and position information
- `dir`: ScanDirection indicating whether to scan forward or backward

## Dependencies
- Functions called/Symbols referenced:
  - ScanDirectionIsForward
  - [_bt_steppage](_bt_steppage.md)
- Called from:
  - [btgettuple](btgettuple.md)
  - [btgetbitmap](btgetbitmap.md)

## Notes and Other Information
- Returns true if a next tuple is found, false if the scan is complete
- Manages the itemIndex within the current scan position to track progress through current page
- Automatically transitions to the next page when the current page is exhausted
- Sets scan->xs_heaptid to the heap TID of the current tuple
- Optionally sets scan->xs_itup to point to a copy of the index tuple if requested
- On failure (no more tuples), releases the pin and sets currPos.buf to InvalidBuffer
- Works in conjunction with _bt_steppage() for cross-page navigation
- Essential for implementing efficient sequential access through B-tree scan results
- Much simpler than _bt_first() since the scan positioning and key matching logic is already established
- Maintains proper buffer management and locking discipline during scan progression