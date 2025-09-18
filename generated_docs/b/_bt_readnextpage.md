# _bt_readnextpage

## Location
[src/backend/access/nbtree/nbtsearch.c:2181-2346](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsearch.c#L2181-L2346)

## Overview
Reads the next page containing valid data for a B-tree index scan, handling both forward and backward directions with parallel scan coordination.

## Definition
```c
static bool _bt_readnextpage(IndexScanDesc scan, BlockNumber blkno, ScanDirection dir)
```

## Detailed Description
This function is responsible for navigating to and reading the next page of data during B-tree index scanning. It handles the complexities of page traversal in both forward and backward directions, manages parallel scan coordination, deals with deleted pages, and ensures proper buffer management and locking. For forward scans, it follows right-links between pages, while backward scans use the more complex _bt_walk_left algorithm to handle concurrent page splits and deletions. The function integrates with PostgreSQL's parallel scanning infrastructure and includes predicate locking for proper isolation.

## Parameters / Member Variables
- `scan`: Index scan descriptor containing scan state and configuration
- `blkno`: Block number of the page to read next
- `dir`: Scan direction (forward or backward)

## Dependencies
- Functions called/Symbols referenced:
  - [_bt_parallel_done](_bt_parallel_done.md), _bt_parallel_seize, _bt_parallel_release (parallel scan coordination)
  - [_bt_getbuf](_bt_getbuf.md), _bt_relbuf, _bt_lockbuf (buffer management)
  - [_bt_readpage](_bt_readpage.md) (page content processing)
  - [_bt_walk_left](_bt_walk_left.md) (backward page navigation)
  - BTScanPosInvalidate, BTScanPosUnpinIfPinned, BTScanPosIsPinned (scan position management)
  - [PredicateLockPage](../P/PredicateLockPage.md) (isolation/locking)
  - Various page and buffer utility functions
- Called from (representative examples):
  - [_bt_steppage](_bt_steppage.md) (primary page stepping logic)
  - [_bt_parallel_readpage](_bt_parallel_readpage.md) (parallel scan context)

## Notes and Other Information
- Returns true if a valid next page with matching data was found, false if the scan has reached its end
- Forward scans are straightforward, following btpo_next links, while backward scans are complex due to concurrent modifications
- Handles deleted/half-dead pages by skipping them and continuing the search
- Includes comprehensive parallel scan support with proper coordination between workers
- Updates the scan position's moreLeft/moreRight indicators based on scan results
- Critical for maintaining scan consistency across page boundaries in a concurrent environment
- The backward scan logic implements the algorithm described in nbtree/README for handling concurrent page splits and deletions