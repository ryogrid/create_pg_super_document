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

## Simplified Source

```c
static bool
_bt_readnextpage(IndexScanDesc scan, BlockNumber blkno, ScanDirection dir)
{
    BTScanOpaque so = (BTScanOpaque) scan->opaque;
    Relation rel = scan->indexRelation;
    Page page;
    BTPageOpaque opaque;

    if (ScanDirectionIsForward(dir)) {
        // Forward scan: follow right links
        for (;;) {
            // Check for end of scan
            if (blkno == P_NONE || !so->currPos.moreRight) {
                _bt_parallel_done(scan);
                BTScanPosInvalidate(so->currPos);
                return false;
            }

            CHECK_FOR_INTERRUPTS();

            // Read next page
            so->currPos.buf = _bt_getbuf(rel, blkno, BT_READ);
            page = BufferGetPage(so->currPos.buf);
            opaque = BTPageGetOpaque(page);

            // Skip deleted pages, process valid pages
            if (!P_IGNORE(opaque)) {
                PredicateLockPage(rel, blkno, scan->xs_snapshot);
                if (_bt_readpage(scan, dir, P_FIRSTDATAKEY(opaque), false))
                    break;  // Found matching data
            } else if (scan->parallel_scan != NULL) {
                _bt_parallel_release(scan, opaque->btpo_next);
            }

            // Move to next page
            if (scan->parallel_scan != NULL) {
                _bt_relbuf(rel, so->currPos.buf);
                if (!_bt_parallel_seize(scan, &blkno, false)) {
                    BTScanPosInvalidate(so->currPos);
                    return false;
                }
            } else {
                blkno = opaque->btpo_next;
                _bt_relbuf(rel, so->currPos.buf);
            }
        }
    } else {
        // Backward scan: use complex left-walking algorithm
        if (so->currPos.currPage != blkno) {
            BTScanPosUnpinIfPinned(so->currPos);
            so->currPos.currPage = blkno;
        }

        // Get buffer with appropriate locking
        if (BTScanPosIsPinned(so->currPos))
            _bt_lockbuf(rel, so->currPos.buf, BT_READ);
        else
            so->currPos.buf = _bt_getbuf(rel, so->currPos.currPage, BT_READ);

        for (;;) {
            // Check for end of scan
            if (!so->currPos.moreLeft) {
                _bt_relbuf(rel, so->currPos.buf);
                _bt_parallel_done(scan);
                BTScanPosInvalidate(so->currPos);
                return false;
            }

            // Walk left to previous page
            so->currPos.buf = _bt_walk_left(rel, so->currPos.buf);
            if (so->currPos.buf == InvalidBuffer) {
                _bt_parallel_done(scan);
                BTScanPosInvalidate(so->currPos);
                return false;
            }

            // Process page if valid
            page = BufferGetPage(so->currPos.buf);
            opaque = BTPageGetOpaque(page);
            if (!P_IGNORE(opaque)) {
                PredicateLockPage(rel, BufferGetBlockNumber(so->currPos.buf), scan->xs_snapshot);
                if (_bt_readpage(scan, dir, PageGetMaxOffsetNumber(page), false))
                    break;  // Found matching data
            } else if (scan->parallel_scan != NULL) {
                _bt_parallel_release(scan, BufferGetBlockNumber(so->currPos.buf));
            }

            // Coordinate with parallel workers
            if (scan->parallel_scan != NULL) {
                _bt_relbuf(rel, so->currPos.buf);
                if (!_bt_parallel_seize(scan, &blkno, false)) {
                    BTScanPosInvalidate(so->currPos);
                    return false;
                }
                so->currPos.buf = _bt_getbuf(rel, blkno, BT_READ);
            }
        }
    }

    return true;
}
```