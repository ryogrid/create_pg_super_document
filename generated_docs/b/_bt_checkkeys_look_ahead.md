# _bt_checkkeys_look_ahead

## Location
[src/backend/access/nbtree/nbtutils.c:4072-4170](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtutils.c#L4072-L4170)

## Overview
Optimizes B-tree array key scans by performing look-ahead analysis to skip over uninteresting tuples that fall between array key values.

## Definition

```c
struct _bt_readpage to skip ahead to very next tuple
		 * after the one we determined was still before the current array keys
		 */
		if (ScanDirectionIsForward(dir))
			pstate->skip = aheadoffnum + 1;
```
## Detailed Description
This function implements a look-ahead optimization for B-tree scans with array keys. When scanning encounters a gap between matching array keys (where many tuples exist that don't match any array values), this function attempts to skip ahead to find the next potentially matching tuple. It uses adaptive heuristics to determine how far ahead to look, starting with a small distance and exponentially increasing it when successful skips are found. The function checks a tuple at the target distance to see if it's still "before" the current array keys, and if so, instructs the scan to skip ahead to that position. This optimization is particularly effective for sparse array key matches on large pages.

## Parameters / Member Variables
- : Index scan descriptor containing scan state and array keys
- : B-tree read page state containing current position and skip information
- : Number of attributes in tuples being scanned
- : Tuple descriptor defining attribute types

## Dependencies
- Functions called/Symbols referenced:
  - [_bt_tuple_before_array_skeys](_bt_tuple_before_array_skeys.md)
  - [PageGetItem](../P/PageGetItem.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - ScanDirectionIsForward/ScanDirectionIsBackward
  - LOOK_AHEAD_DEFAULT_DISTANCE
  - MaxIndexTuplesPerPage
- Called from (representative examples):
  - [_bt_checkkeys](_bt_checkkeys.md)

## Notes and Other Information
- Uses adaptive distance heuristics: starts with LOOK_AHEAD_DEFAULT_DISTANCE, doubles on success up to MaxIndexTuplesPerPage/2
- Reduces distance aggressively (by factor of 8) when look-ahead fails
- Only operates when sufficient tuples remain on the page for meaningful optimization
- Sets pstate->skip to instruct _bt_readpage where to continue scanning
- Critical optimization for array key performance in PostgreSQL B-tree indexes
- Located in src/backend/access/nbtree/nbtutils.c:4072-4170

## Simplified Source

```c
static void
_bt_checkkeys_look_ahead(IndexScanDesc scan, BTReadPageState *pstate,
                         int tupnatts, TupleDesc tupdesc)
{
    ScanDirection dir = pstate->dir;
    OffsetNumber aheadoffnum;
    IndexTuple ahead;

    // Skip if comparing page high key or insufficient tuples remain
    if (pstate->offnum < pstate->minoff)
        return;

    if (ScanDirectionIsForward(dir) &&
        pstate->offnum >= pstate->maxoff - LOOK_AHEAD_DEFAULT_DISTANCE)
        return;
    else if (ScanDirectionIsBackward(dir) &&
             pstate->offnum <= pstate->minoff + LOOK_AHEAD_DEFAULT_DISTANCE)
        return;

    // Adaptive distance heuristics: start small, ramp up on success
    if (!pstate->targetdistance)
        pstate->targetdistance = LOOK_AHEAD_DEFAULT_DISTANCE;
    else if (pstate->targetdistance < MaxIndexTuplesPerPage / 2)
        pstate->targetdistance *= 2;

    // Calculate look-ahead offset, staying within page bounds
    if (ScanDirectionIsForward(dir))
        aheadoffnum = Min((int) pstate->maxoff,
                         (int) pstate->offnum + pstate->targetdistance);
    else
        aheadoffnum = Max((int) pstate->minoff,
                         (int) pstate->offnum - pstate->targetdistance);

    // Get the tuple at look-ahead position
    ahead = (IndexTuple) PageGetItem(pstate->page,
                                    PageGetItemId(pstate->page, aheadoffnum));

    // Test if the ahead tuple is still before current array keys
    if (_bt_tuple_before_array_skeys(scan, dir, ahead, tupdesc, tupnatts,
                                    false, 0, NULL))
    {
        // Success: skip to next tuple after the ahead position
        if (ScanDirectionIsForward(dir))
            pstate->skip = aheadoffnum + 1;
        else
            pstate->skip = aheadoffnum - 1;
    }
    else
    {
        // Failure: ahead tuple is too far, reduce target distance aggressively
        pstate->rechecks = 0;
        pstate->targetdistance = Max(pstate->targetdistance / 8, 1);
    }
}
```