# _bt_verify_keys_with_arraykeys

## Location
[src/backend/access/nbtree/nbtutils.c:3044-3121](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtutils.c#L3044-L3121)

## Overview
Verifies that the scan's keyData[] scan keys are in agreement with its array key state, ensuring consistency between scan keys and array key metadata during B-tree index scans.

## Definition
static bool _bt_verify_keys_with_arraykeys(IndexScanDesc scan)

## Detailed Description
This internal B-tree function performs integrity checking to ensure that the scan keys stored in so->keyData[] are consistent with the array key state maintained in so->arrayKeys[]. It validates that:

1. Array scan keys have the BTEqualStrategyNumber strategy and SK_SEARCHARRAY flag
2. Array key metadata correctly corresponds to scan key positions
3. Array elements have valid counts and current values
4. Scan key attributes are in ascending order
5. The total number of array keys matches expectations

The function is primarily used for debugging and assertion purposes to catch inconsistencies in array key processing during B-tree scans.

## Parameters / Member Variables
- : IndexScanDesc - The index scan descriptor containing scan keys and array key state to verify

## Dependencies
- Functions called/Symbols referenced:
  - BTScanOpaque (type cast)
  - InvalidAttrNumber (constant)
  - BTEqualStrategyNumber (strategy constant)
  - SK_SEARCHARRAY (scan key flag)
  - [BTArrayKeyInfo](../B/BTArrayKeyInfo.md) (array key metadata structure)
  - ScanKey (scan key structure)

- Called from (representative examples):
  - [_bt_advance_array_keys](_bt_advance_array_keys.md) (validation during array advancement)
  - [_bt_preprocess_keys](_bt_preprocess_keys.md) (validation during key preprocessing)
  - [_bt_verify_arrays_bt_first](_bt_verify_arrays_bt_first.md) (validation in first scan positioning)

## Notes and Other Information
- This is a static debugging function that returns false if any inconsistency is detected
- Only processes scan keys with BTEqualStrategyNumber strategy and SK_SEARCHARRAY flag
- Expects scan key attributes to be in ascending order (last_sk_attno <= cur->sk_attno)
- Validates that the current array element value matches the scan key argument
- Used primarily for internal consistency checking and debugging purposes
- Part of PostgreSQL's B-tree array key optimization feature

## Simplified Source

```c
static bool
_bt_verify_keys_with_arraykeys(IndexScanDesc scan)
{
    BTScanOpaque so = (BTScanOpaque) scan->opaque;
    int last_sk_attno = InvalidAttrNumber;
    int arrayidx = 0;

    // Check if scan state is valid
    if (!so->qual_ok)
        return false;

    // Verify each scan key's array state
    for (int ikey = 0; ikey < so->numberOfKeys; ikey++)
    {
        ScanKey cur = so->keyData + ikey;
        BTArrayKeyInfo *array;

        // Only process equality array keys
        if (cur->sk_strategy != BTEqualStrategyNumber ||
            !(cur->sk_flags & SK_SEARCHARRAY))
            continue;

        array = &so->arrayKeys[arrayidx++];

        // Verify array metadata consistency
        if (array->scan_key != ikey)
            return false;

        if (array->num_elems <= 0)
            return false;

        // Check current array element matches scan key argument
        if (cur->sk_argument != array->elem_values[array->cur_elem])
            return false;

        // Verify attributes are in ascending order
        if (last_sk_attno > cur->sk_attno)
            return false;
        last_sk_attno = cur->sk_attno;
    }

    // Final check: array count should match expected
    if (arrayidx != so->numArrayKeys)
        return false;

    return true;
}
```