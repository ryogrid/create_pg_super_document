# _bt_steppage

## Location
[src/backend/access/nbtree/nbtsearch.c:2041-2180](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsearch.c#L2041-L2180)

## Overview
Steps to the next page containing valid data during B-tree index scanning, handling page transitions and maintaining scan state consistency.

## Definition
```c
static bool _bt_steppage(IndexScanDesc scan, ScanDirection dir)
```

## Detailed Description
This function manages page transitions during B-tree index scans. It handles several critical responsibilities: processing any killed items on the current page, preserving mark position state if needed, managing buffer pins and locks appropriately, coordinating with parallel scans when applicable, and determining the next page to scan based on direction. The function ensures proper cleanup of the current page state while setting up for continued scanning on the next page.

## Parameters / Member Variables
- `scan`: Index scan descriptor containing scan state and configuration
- `dir`: Scan direction (forward or backward) indicating which direction to step

## Dependencies
- Functions called/Symbols referenced:
  - [_bt_killitems](_bt_killitems.md) (process killed items)
  - [_bt_parallel_seize](_bt_parallel_seize.md) (coordinate with parallel scans)
  - [_bt_readnextpage](_bt_readnextpage.md) (read and setup next page)
  - [_bt_drop_lock_and_maybe_pin](_bt_drop_lock_and_maybe_pin.md) (manage buffer locks/pins)
  - BTScanPosIsValid, BTScanPosIsPinned, BTScanPosUnpinIfPinned, BTScanPosInvalidate (scan position state management)
  - [IncrBufferRefCount](../I/IncrBufferRefCount.md) (buffer reference counting)
  - ScanDirectionIsForward (direction checking)
- Called from (representative examples):
  - [_bt_first](_bt_first.md) (initial scan setup)
  - [_bt_next](_bt_next.md) (scan continuation)
  - [_bt_endpoint](_bt_endpoint.md) (scan termination)

## Notes and Other Information
- Returns true if successfully stepped to a valid next page, false if scan has ended
- Handles complex mark/restore functionality by preserving current position state when mark positions are active
- Manages both parallel and non-parallel scan scenarios with different page navigation strategies
- Includes logic to handle array key scans and primitive index scan cancellation
- Properly coordinates buffer management, ensuring pins are maintained correctly between page transitions
- The function maintains scan consistency by updating moreLeft/moreRight indicators based on scan direction and page transitions

## Simplified Source

```c
static bool
_bt_steppage(IndexScanDesc scan, ScanDirection dir)
{
    BTScanOpaque so = (BTScanOpaque) scan->opaque;
    BlockNumber blkno = InvalidBlockNumber;

    // Process any killed items before leaving current page
    if (so->numKilled > 0)
        _bt_killitems(scan);

    // Save current position state if mark position is active
    if (so->markItemIndex >= 0) {
        if (BTScanPosIsPinned(so->currPos))
            IncrBufferRefCount(so->currPos.buf);

        // Copy current position to mark position
        memcpy(&so->markPos, &so->currPos,
               offsetof(BTScanPosData, items[1]) + so->currPos.lastItem * sizeof(BTScanPosItem));

        if (so->markTuples)
            memcpy(so->markTuples, so->currTuples, so->currPos.nextTupleOffset);

        so->markPos.itemIndex = so->markItemIndex;
        so->markItemIndex = -1;

        // Handle array scan direction changes
        if (so->needPrimScan) {
            if (ScanDirectionIsForward(so->currPos.dir))
                so->markPos.moreRight = true;
            else
                so->markPos.moreLeft = true;
        }
    }

    // Cancel primitive scans if direction changed
    if (so->currPos.dir != dir)
        so->needPrimScan = false;

    // Determine next page based on scan direction
    if (ScanDirectionIsForward(dir)) {
        // Get next page (parallel or sequential)
        if (scan->parallel_scan != NULL) {
            if (!_bt_parallel_seize(scan, &blkno, false)) {
                BTScanPosUnpinIfPinned(so->currPos);
                BTScanPosInvalidate(so->currPos);
                return false;
            }
        } else {
            blkno = so->currPos.nextPage;
        }
        so->currPos.moreLeft = true;
        BTScanPosUnpinIfPinned(so->currPos);
    } else {
        // Backward scan
        so->currPos.moreRight = true;
        if (scan->parallel_scan != NULL) {
            BTScanPosUnpinIfPinned(so->currPos);
            if (!_bt_parallel_seize(scan, &blkno, false)) {
                BTScanPosInvalidate(so->currPos);
                return false;
            }
        } else {
            blkno = so->currPos.currPage;
        }
    }

    // Read next page and setup for continued scanning
    if (!_bt_readnextpage(scan, blkno, dir))
        return false;

    _bt_drop_lock_and_maybe_pin(scan, &so->currPos);
    return true;
}
```