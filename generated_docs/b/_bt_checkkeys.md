# _bt_checkkeys

## Location
[src/backend/access/nbtree/nbtutils.c:3508-3681](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtutils.c#L3508-L3681)

## Overview
Tests whether an index tuple satisfies all scankey conditions and manages array key advancement and scan continuation decisions during B-tree index scans.

## Definition
bool _bt_checkkeys(IndexScanDesc scan, BTReadPageState *pstate, bool arrayKeys, IndexTuple tuple, int tupnatts)

## Detailed Description
This function serves as the primary entry point for tuple qualification during B-tree scans. It performs several critical operations:

1. **Tuple qualification**: Tests whether the given index tuple satisfies all scan key conditions using _bt_check_compare.

2. **Scan continuation logic**: Determines whether the scan needs to continue beyond the current tuple by setting pstate->continuescan appropriately.

3. **Array key advancement**: For scans with array keys (arrayKeys=true), advances the scan's array keys when necessary and handles complex array key positioning scenarios.

4. **Page boundary optimization**: Accepts high key tuples from forward scan callers to potentially set continuescan=false and avoid unnecessary page visits.

5. **Primitive scan management**: Starts and stops primitive index scans for array key scenarios, including recovery mechanisms when scans fall significantly behind array key positions.

6. **Look-ahead optimization**: Implements speculative look-ahead within leaf pages to minimize linear search overhead when array keys are involved.

## Parameters / Member Variables
- : IndexScanDesc - The index scan descriptor containing search conditions
- : BTReadPageState * - Page-level input/output parameters including continuation state
- : bool - Whether to advance array keys if necessary (false for precheck calls)
- : IndexTuple - The index tuple to test against scan conditions
- : int - Number of attributes in the tuple (may be truncated for high keys)

## Dependencies
- Functions called/Symbols referenced:
  - [_bt_check_compare](_bt_check_compare.md) (core tuple comparison logic)
  - [_bt_tuple_before_array_skeys](_bt_tuple_before_array_skeys.md) (array key positioning checks)
  - [_bt_advance_array_keys](_bt_advance_array_keys.md) (array key advancement)
  - [_bt_checkkeys_look_ahead](_bt_checkkeys_look_ahead.md) (speculative page scanning optimization)
  - BTreeTupleGetNAtts (tuple attribute counting)
  - RelationGetDescr (relation descriptor access)
  - BTScanOpaque, BTReadPageState, ScanDirection (scan state structures)
  - LOOK_AHEAD_REQUIRED_RECHECKS (optimization threshold constant)

- Called from (representative examples):
  - [_bt_readpage](_bt_readpage.md) (multiple calls during page scanning for tuple qualification and continuation decisions)

## Notes and Other Information
- Returns true if tuple satisfies all conditions, false otherwise
- Central to B-tree scan efficiency through early termination and array key optimization
- Handles complex array key scenarios including scan recovery when position falls behind
- Supports both forward and backward scan directions
- Includes extensive assertion checking for debugging array key logic
- The arrayKeys parameter allows callers to perform precheck operations without array side-effects
- Page state management requires proper setup of finaltup for array key scans
- Look-ahead optimization prevents excessive linear searching when array keys advance frequently
- Part of PostgreSQL's advanced B-tree array key optimization system

## Simplified Source

```c
bool
_bt_checkkeys(IndexScanDesc scan, BTReadPageState *pstate, bool arrayKeys,
              IndexTuple tuple, int tupnatts)
{
    TupleDesc tupdesc = RelationGetDescr(scan->indexRelation);
    BTScanOpaque so = (BTScanOpaque) scan->opaque;
    ScanDirection dir = pstate->dir;
    int ikey = 0;
    bool res;

    // Primary tuple qualification check
    res = _bt_check_compare(scan, dir, tuple, tupnatts, tupdesc,
                           arrayKeys, pstate->prechecked, pstate->firstmatch,
                           &pstate->continuescan, &ikey);

    // For non-array scans or when scan can continue, return result
    if (!arrayKeys || pstate->continuescan)
        return res;

    // Handle array key advancement when continuescan=false
    // Check if tuple is before current array keys
    if (_bt_tuple_before_array_skeys(scan, dir, tuple, tupdesc, tupnatts, true,
                                    ikey, NULL))
    {
        // Handle scan-behind condition for recovery
        if (unlikely(so->scanBehind) && pstate->finaltup &&
            _bt_tuple_before_array_skeys(scan, dir, pstate->finaltup, tupdesc,
                                        BTreeTupleGetNAtts(pstate->finaltup,
                                                          scan->indexRelation),
                                        false, 0, NULL))
        {
            // Start new primitive scan
            pstate->continuescan = false;
            so->needPrimScan = true;
        }
        else
        {
            // Continue current scan
            pstate->continuescan = true;

            // Use look-ahead optimization if needed
            pstate->rechecks++;
            if (pstate->rechecks >= LOOK_AHEAD_REQUIRED_RECHECKS)
            {
                _bt_checkkeys_look_ahead(scan, pstate, tupnatts, tupdesc);
            }
        }

        return false;  // Tuple doesn't match current conditions
    }

    // Tuple is at/past current array keys - advance arrays
    return _bt_advance_array_keys(scan, pstate, tuple, tupnatts, tupdesc,
                                 ikey, true);
}
```