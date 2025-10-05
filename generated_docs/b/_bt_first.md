# _bt_first

## Location
[src/backend/access/nbtree/nbtsearch.c:876-1495](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsearch.c#L876-L1495)

## Overview
Finds the first item in a B-tree scan, positioning the scan at the appropriate starting point based on the scan keys and direction, handling complex scenarios like parallel scans and array keys.

## Definition
```c
bool _bt_first(IndexScanDesc scan, ScanDirection dir)
```

## Detailed Description
This function is the entry point for initializing a B-tree index scan. It determines where to start the scan based on the provided scan keys and direction, then positions the scan at the appropriate tuple. The function handles multiple complex scenarios including parallel scans, array keys, boundary key selection, and various scan strategies (equality, range queries, etc.).

The function processes scan keys to build an insertion-type scan key for tree traversal, handles special cases like NULL values and row comparisons, and sets up the scan state for subsequent _bt_next() calls. It supports both forward and backward scans and manages parallel scan coordination when multiple workers are involved.

## Parameters / Member Variables
- `scan`: IndexScanDesc containing the scan state and parameters
- `dir`: ScanDirection indicating forward or backward scan direction

## Dependencies
- Functions called/Symbols referenced:
  - [_bt_preprocess_keys](_bt_preprocess_keys.md)
  - [_bt_parallel_seize](_bt_parallel_seize.md)
  - [_bt_parallel_readpage](_bt_parallel_readpage.md)
  - [_bt_start_array_keys](_bt_start_array_keys.md)
  - [_bt_endpoint](_bt_endpoint.md)
  - [_bt_search](_bt_search.md)
  - [_bt_binsrch](_bt_binsrch.md)
  - [_bt_readpage](_bt_readpage.md)
  - [_bt_steppage](_bt_steppage.md)
  - [_bt_metaversion](_bt_metaversion.md)
  - [_bt_initialize_more_data](_bt_initialize_more_data.md)
  - [_bt_unlockbuf](_bt_unlockbuf.md)
  - [_bt_drop_lock_and_maybe_pin](_bt_drop_lock_and_maybe_pin.md)
  - [_bt_freestack](_bt_freestack.md)
  - [_bt_parallel_done](_bt_parallel_done.md)
  - pgstat_count_index_scan
  - [PredicateLockRelation](../P/PredicateLockRelation.md)
  - [PredicateLockPage](../P/PredicateLockPage.md)
- Called from:
  - [btgettuple](btgettuple.md)
  - [btgetbitmap](btgetbitmap.md)

## Notes and Other Information
- Returns true if a matching tuple is found, false if no matches exist
- Handles parallel scan coordination and load balancing across multiple workers
- Processes complex scan key combinations including row comparisons and array keys
- Implements boundary key selection logic for optimal scan starting positions
- Sets up scan positioning for various strategies (=, <, <=, >, >=)
- Manages buffer locking and predicate locking for proper concurrency control
- Critical for B-tree scan performance as it determines the optimal starting point
- The function can handle scans that start from either end of the tree when no boundary keys are available
- Supports cross-type comparisons and handles opfamily procedure lookups
- Essential for implementing PostgreSQL's index scan access methods efficiently

## Simplified Source

```c
bool
_bt_first(IndexScanDesc scan, ScanDirection dir)
{
    Relation rel = scan->indexRelation;
    BTScanOpaque so = (BTScanOpaque) scan->opaque;
    Buffer buf;
    BTStack stack;
    OffsetNumber offnum;
    BTScanInsertData inskey;
    ScanKey startKeys[INDEX_MAX_KEYS];
    int keysz = 0;
    StrategyNumber strat_total;
    BTScanPosItem *currItem;
    BlockNumber blkno;

    // Process and validate scan keys
    _bt_preprocess_keys(scan);

    if (!so->qual_ok)
    {
        _bt_parallel_done(scan);
        return false;
    }

    // Handle parallel scans
    if (scan->parallel_scan != NULL)
    {
        bool status = _bt_parallel_seize(scan, &blkno, true);

        if (so->numArrayKeys && !so->needPrimScan)
            _bt_start_array_keys(scan, dir);

        if (!status)
            return false;
        else if (blkno == P_NONE)
        {
            _bt_parallel_done(scan);
            return false;
        }
        else if (blkno != InvalidBlockNumber)
        {
            if (!_bt_parallel_readpage(scan, blkno, dir))
                return false;
            goto readcomplete;
        }
    }
    else if (so->numArrayKeys && !so->needPrimScan)
    {
        _bt_start_array_keys(scan, dir);
    }

    pgstat_count_index_scan(rel);

    // Build boundary keys for scan positioning
    strat_total = BTEqualStrategyNumber;
    if (so->numberOfKeys > 0)
    {
        // Simplified key processing - select appropriate boundary keys
        // based on scan direction and strategy
        for (int i = 0; i < so->numberOfKeys && keysz < INDEX_MAX_KEYS; i++)
        {
            ScanKey cur = &so->keyData[i];

            // Select usable boundary keys based on strategy and direction
            switch (cur->sk_strategy)
            {
                case BTEqualStrategyNumber:
                    startKeys[keysz++] = cur;
                    break;
                case BTGreaterEqualStrategyNumber:
                case BTGreaterStrategyNumber:
                    if (ScanDirectionIsForward(dir))
                    {
                        startKeys[keysz++] = cur;
                        strat_total = cur->sk_strategy;
                    }
                    break;
                case BTLessEqualStrategyNumber:
                case BTLessStrategyNumber:
                    if (ScanDirectionIsBackward(dir))
                    {
                        startKeys[keysz++] = cur;
                        strat_total = cur->sk_strategy;
                    }
                    break;
            }
        }
    }

    // If no boundary keys, scan from tree endpoint
    if (keysz == 0)
    {
        bool match = _bt_endpoint(scan, dir);
        if (!match)
            _bt_parallel_done(scan);
        return match;
    }

    // Build insertion scan key for tree search
    _bt_metaversion(rel, &inskey.heapkeyspace, &inskey.allequalimage);
    inskey.anynullkeys = false;
    inskey.scantid = NULL;
    inskey.keysz = keysz;

    // Set search direction based on strategy
    switch (strat_total)
    {
        case BTLessStrategyNumber:
            inskey.nextkey = false;
            inskey.backward = true;
            break;
        case BTLessEqualStrategyNumber:
            inskey.nextkey = true;
            inskey.backward = true;
            break;
        case BTEqualStrategyNumber:
            if (ScanDirectionIsBackward(dir))
            {
                inskey.nextkey = true;
                inskey.backward = true;
            }
            else
            {
                inskey.nextkey = false;
                inskey.backward = false;
            }
            break;
        case BTGreaterEqualStrategyNumber:
            inskey.nextkey = false;
            inskey.backward = false;
            break;
        case BTGreaterStrategyNumber:
            inskey.nextkey = true;
            inskey.backward = false;
            break;
    }

    // Search to target leaf page
    stack = _bt_search(rel, NULL, &inskey, &buf, BT_READ);
    _bt_freestack(stack);

    if (!BufferIsValid(buf))
    {
        // Handle empty index
        if (IsolationIsSerializable())
        {
            PredicateLockRelation(rel, scan->xs_snapshot);
            stack = _bt_search(rel, NULL, &inskey, &buf, BT_READ);
            _bt_freestack(stack);
        }

        if (!BufferIsValid(buf))
        {
            _bt_parallel_done(scan);
            BTScanPosInvalidate(so->currPos);
            return false;
        }
    }

    PredicateLockPage(rel, BufferGetBlockNumber(buf), scan->xs_snapshot);
    _bt_initialize_more_data(so, dir);

    // Position to precise item on page
    offnum = _bt_binsrch(rel, &inskey, buf);
    so->currPos.buf = buf;

    // Read page data
    if (!_bt_readpage(scan, dir, offnum, true))
    {
        _bt_unlockbuf(scan->indexRelation, so->currPos.buf);
        if (!_bt_steppage(scan, dir))
            return false;
    }
    else
    {
        _bt_drop_lock_and_maybe_pin(scan, &so->currPos);
    }

readcomplete:
    // Set up scan result
    currItem = &so->currPos.items[so->currPos.itemIndex];
    scan->xs_heaptid = currItem->heapTid;
    if (scan->xs_want_itup)
        scan->xs_itup = (IndexTuple) (so->currTuples + currItem->tupleOffset);

    return true;
}
```