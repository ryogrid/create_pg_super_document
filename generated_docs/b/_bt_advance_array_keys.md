# _bt_advance_array_keys

## Location
[src/backend/access/nbtree/nbtutils.c:1789-2551](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtutils.c#L1789-L2551)

## Overview
Advances array elements using a tuple to determine new array positions, and serves as a wrapper around _bt_check_compare for requalification checks.

## Definition

```c
static bool
_bt_advance_array_keys(IndexScanDesc scan, BTReadPageState *pstate,
					   IndexTuple tuple, int tupnatts, TupleDesc tupdesc,
					   int sktrig, bool sktrig_required)
```
## Detailed Description
This function is the core of PostgreSQL's B-tree array key advancement mechanism. It performs a complex process of advancing array scan keys based on tuple values, ensuring that the scan progresses correctly through the index while handling both required and non-required array keys.

The function operates by comparing each array element against the corresponding tuple attribute values and finding the appropriate next array position. It implements a "ratcheting" mechanism where required arrays can only advance forward (or backward in reverse scans) and never retreat. The function handles several scenarios:

1. **Exact matches**: When tuple values exactly match array elements
2. **Beyond-end advancement**: When tuple values exceed the closest array elements
3. **Rollover handling**: When advancement needs to carry to higher-order arrays
4. **Non-required arrays**: Special handling for optional array keys

The function also performs requalification by calling _bt_check_compare with the newly advanced keys to determine if the original tuple still satisfies the updated scan criteria. It can recursively call itself for "second pass" handling of required inequality scan keys that weren't initially detected.

## Parameters / Member Variables
- `scan`: Index scan descriptor containing array key state and scan information
- `*pstate`: Page-level scan state for tracking page boundaries and scan direction
- `tuple`: The index tuple that triggered array advancement
- `tupnatts`: Number of attributes in the tuple
- `tupdesc`: Tuple descriptor for attribute access
- `sktrig`: Index of the scan key that triggered the advancement
- `sktrig_required`: Whether the triggering scan key is required in the current scan direction
## Dependencies
- Functions called/Symbols referenced:
  - [_bt_tuple_before_array_skeys](_bt_tuple_before_array_skeys.md)
  - [_bt_verify_keys_with_arraykeys](_bt_verify_keys_with_arraykeys.md)  
  - [_bt_binsrch_array_skey](_bt_binsrch_array_skey.md)
  - [_bt_compare_array_skey](_bt_compare_array_skey.md)
  - [_bt_advance_array_keys_increment](_bt_advance_array_keys_increment.md)
  - [_bt_check_compare](_bt_check_compare.md)
  - [_bt_rewind_nonrequired_arrays](_bt_rewind_nonrequired_arrays.md)
  - [_bt_parallel_primscan_schedule](_bt_parallel_primscan_schedule.md)
  - [index_getattr](../i/index_getattr.md)
  - BTreeTupleGetNAtts
- Called from (representative examples):
  - [_bt_checkkeys](_bt_checkkeys.md)
  - [_bt_check_compare](_bt_check_compare.md)
  - [_bt_advance_array_keys](_bt_advance_array_keys.md) (recursive)

## Notes and Other Information
- Implements a complex state machine for array advancement with multiple phases
- Handles truncated attributes in high key tuples by setting scanBehind flag
- Supports both forward and backward scan directions with direction-specific logic
- Can trigger new primitive index scans when arrays become exhausted or when optimizations indicate page skipping opportunities
- Uses binary search for finding closest matching array elements
- Maintains strict ordering guarantees: arrays never advance beyond what's safe based on current tuple information
- Includes extensive optimization logic for handling opposite-direction inequality keys and NULL value boundaries
- Return value indicates whether the triggering tuple satisfies the newly advanced array keys

## Simplified Source

```c
static bool
_bt_advance_array_keys(IndexScanDesc scan, BTReadPageState *pstate,
                      IndexTuple tuple, int tupnatts, TupleDesc tupdesc,
                      int sktrig, bool sktrig_required)
{
    BTScanOpaque so = (BTScanOpaque) scan->opaque;
    Relation rel = scan->indexRelation;
    ScanDirection dir = pstate ? pstate->dir : ForwardScanDirection;
    int arrayidx = 0;
    bool beyond_end_advance = false;
    bool all_required_satisfied = true, all_satisfied = true;

    // Reset scanBehind flag
    so->scanBehind = false;

    if (sktrig_required) {
        // Verify precondition: tuple >= current array keys
        Assert(!_bt_tuple_before_array_skeys(scan, dir, tuple, tupdesc,
                                            tupnatts, false, 0, NULL));

        // Invalidate page-level optimization state
        pstate->firstmatch = false;
        pstate->rechecks = 0;
        pstate->targetdistance = 0;
    }

    // Process each scan key to advance arrays
    for (int ikey = 0; ikey < so->numberOfKeys; ikey++) {
        ScanKey cur = so->keyData + ikey;
        BTArrayKeyInfo *array = NULL;
        Datum tupdatum;
        bool required = false, tupnull;
        int32 result;
        int set_elem = 0;

        // Handle equality strategy scan keys with arrays
        if (cur->sk_strategy == BTEqualStrategyNumber &&
            (cur->sk_flags & SK_SEARCHARRAY)) {
            array = &so->arrayKeys[arrayidx++];
            Assert(array->scan_key == ikey);
        }

        // Skip already satisfied keys (optimization)
        if (ikey < sktrig)
            continue;

        // Check if this is a required key
        if (cur->sk_flags & (SK_BT_REQFWD | SK_BT_REQBKWD)) {
            required = true;
            if (cur->sk_attno > tupnatts) {
                // Handle truncated attributes
                so->scanBehind = true;
            }
        }

        // Handle non-array scan key that triggered advancement
        if (ikey == sktrig && !array) {
            beyond_end_advance = true;
            all_satisfied = all_required_satisfied = false;
            continue;
        }

        // Skip non-equality keys that didn't trigger advancement
        if (cur->sk_strategy != BTEqualStrategyNumber)
            continue;

        // Skip non-required, non-array keys
        if (!required && !array)
            continue;

        // Handle beyond-end advancement for subsequent arrays
        if (beyond_end_advance) {
            if (array) {
                int final_elem = ScanDirectionIsBackward(dir) ? 0 : array->num_elems - 1;
                if (array->cur_elem != final_elem) {
                    array->cur_elem = final_elem;
                    cur->sk_argument = array->elem_values[final_elem];
                }
            }
            continue;
        }

        // Handle arrays after unsatisfied required key
        if (!all_required_satisfied || cur->sk_attno > tupnatts) {
            if (array) {
                int first_elem = ScanDirectionIsForward(dir) ? 0 : array->num_elems - 1;
                if (array->cur_elem != first_elem) {
                    array->cur_elem = first_elem;
                    cur->sk_argument = array->elem_values[first_elem];
                }
            }
            continue;
        }

        // Get tuple attribute value
        tupdatum = index_getattr(tuple, cur->sk_attno, tupdesc, &tupnull);

        // Find matching array element
        if (array) {
            bool cur_elem_trig = (sktrig_required && ikey == sktrig);
            set_elem = _bt_binsrch_array_skey(&so->orderProcs[ikey],
                                             cur_elem_trig, dir,
                                             tupdatum, tupnull, array, cur,
                                             &result);
        } else {
            // Non-array equality key - treat as single element array
            result = _bt_compare_array_skey(&so->orderProcs[ikey],
                                           tupdatum, tupnull,
                                           cur->sk_argument, cur);
        }

        // Check if we need beyond-end advancement
        if (required &&
            ((ScanDirectionIsForward(dir) && result > 0) ||
             (ScanDirectionIsBackward(dir) && result < 0))) {
            beyond_end_advance = true;
        }

        // Track satisfaction status
        if (result != 0) {
            all_satisfied = false;
            if (required)
                all_required_satisfied = false;
            else
                break;  // Don't advance non-required arrays further
        }

        // Update array position
        if (array && array->cur_elem != set_elem) {
            array->cur_elem = set_elem;
            cur->sk_argument = array->elem_values[set_elem];
        }
    }

    // Handle beyond-end advancement by incrementing arrays
    if (beyond_end_advance && !_bt_advance_array_keys_increment(scan, dir))
        goto end_toplevel_scan;

    // Recheck tuple against new qual if needed
    if ((sktrig_required && all_required_satisfied) ||
        (!sktrig_required && all_satisfied)) {

        int nsktrig = sktrig + 1;
        bool continuescan;

        // Call _bt_check_compare to verify tuple still matches
        if (_bt_check_compare(scan, dir, tuple, tupnatts, tupdesc,
                             false, false, false,
                             &continuescan, &nsktrig) &&
            !so->scanBehind) {
            // Tuple satisfies new qual
            if (pstate)
                pstate->continuescan = true;
            return true;
        }

        // Handle recursive call for missed inequalities
        if (unlikely(!continuescan)) {
            // Second pass for inequality handling
            _bt_advance_array_keys(scan, pstate, tuple, tupnatts,
                                  tupdesc, nsktrig, true);
            return false;
        }
    }

    // Handle non-required array advancement
    if (!sktrig_required)
        return false;

    // Determine scan continuation strategy
    if (!all_required_satisfied) {
        // Start new primitive scan or continue current page
        if (/* complex conditions for new scan */) {
            goto new_prim_scan;
        }
    }

    // Continue with current page
    pstate->continuescan = true;
    so->needPrimScan = false;
    return false;

new_prim_scan:
    // Schedule new primitive scan
    pstate->continuescan = false;
    so->needPrimScan = true;
    if (scan->parallel_scan)
        _bt_parallel_primscan_schedule(scan, pstate->prev_scan_page);
    return false;

end_toplevel_scan:
    // End scan completely
    pstate->continuescan = false;
    so->needPrimScan = false;
    return false;
}
```