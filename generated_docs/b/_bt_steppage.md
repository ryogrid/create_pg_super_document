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