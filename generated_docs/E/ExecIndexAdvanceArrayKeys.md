# ExecIndexAdvanceArrayKeys

## Location
[src/backend/executor/nodeIndexscan.c:740-784](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeIndexscan.c#L740-L784)

## Overview
Advances to the next combination of array key values for multi-dimensional array-based index scans.

## Definition

```c
bool
ExecIndexAdvanceArrayKeys(IndexArrayKeyInfo *arrayKeys, int numArrayKeys)
```
## Detailed Description
The `ExecIndexAdvanceArrayKeys` function implements a multi-dimensional iterator for array-based index scans. It advances through all possible combinations of array elements in a right-to-left (rightmost-first) manner, similar to an odometer. This function is typically called after `ExecIndexEvalArrayKeys` has initialized the arrays and set up the first combination.

The function uses a rightmost-advance strategy, where the rightmost array key (corresponding to the lowest-order index column) is advanced most frequently. This approach is designed to optimize index locality of access by keeping higher-order index columns stable while iterating through lower-order columns.

The iteration continues until all combinations have been exhausted, at which point the function returns false to indicate that no more combinations are available.

## Parameters
- `arrayKeys`: Array of IndexArrayKeyInfo structures containing the decomposed array elements and iteration state
- `numArrayKeys`: The number of array keys in the arrayKeys array

## Return Value
- `true`: Successfully advanced to next combination; scankeys are updated with new values
- `false`: All combinations have been exhausted; no more iterations possible

## Dependencies
- Data types used:
  - [IndexArrayKeyInfo](../I/IndexArrayKeyInfo.md)  
  - ScanKey
- Constants used:
  - SK_ISNULL

## Called From
- [MultiExecBitmapIndexScan](../M/MultiExecBitmapIndexScan.md) (src/backend/executor/nodeBitmapIndexscan.c:108)

## Notes and Other Information
- Implements rightmost-advance strategy for better index locality of access
- Updates scan key arguments and null flags for each array position
- Resets exhausted arrays to position 0 and continues with next array
- Must be preceded by successful call to ExecIndexEvalArrayKeys
- Critical for implementing efficient array ANY/ALL operations in bitmap index scans
- The iteration pattern ensures all possible combinations of array elements are considered
- Handles null elements correctly by setting/clearing SK_ISNULL flags
- Performance-optimized to advance lowest-order index columns most frequently

## Simplified Source

```c
bool ExecIndexAdvanceArrayKeys(IndexArrayKeyInfo *arrayKeys, int numArrayKeys)
{
    bool found = false;
    int j;

    // Advance arrays right-to-left (odometer style)
    for (j = numArrayKeys - 1; j >= 0; j--) {
        ScanKey scan_key = arrayKeys[j].scan_key;
        int next_elem = arrayKeys[j].next_elem;
        int num_elems = arrayKeys[j].num_elems;
        Datum *elem_values = arrayKeys[j].elem_values;
        bool *elem_nulls = arrayKeys[j].elem_nulls;

        // Check if current array is exhausted
        if (next_elem >= num_elems) {
            next_elem = 0;  // Reset to beginning
            found = false;  // Need to advance next array
        } else {
            found = true;   // Successfully advanced
        }

        // Update scan key with current element
        scan_key->sk_argument = elem_values[next_elem];
        if (elem_nulls[next_elem])
            scan_key->sk_flags |= SK_ISNULL;
        else
            scan_key->sk_flags &= ~SK_ISNULL;

        arrayKeys[j].next_elem = next_elem + 1;

        if (found)
            break;  // Successfully advanced, stop here
    }

    return found;
}
```