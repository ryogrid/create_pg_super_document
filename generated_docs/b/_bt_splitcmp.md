# _bt_splitcmp

## Location
[src/backend/access/nbtree/nbtsplitloc.c:594-629](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsplitloc.c#L594-L629)

## Overview
A qsort-style comparator function that compares two SplitPoint structures based on their delta values to enable sorting split candidates by space balance.

## Definition
```c
static int _bt_splitcmp(const void *arg1, const void *arg2)
```

## Detailed Description
This function serves as the comparison callback for the standard C library `qsort` function when sorting candidate split points. It compares the `curdelta` field of two SplitPoint structures, which represents how balanced each split would be in terms of space utilization.

The function returns:
- Negative value if split1 has a smaller delta (better balanced) than split2
- Zero if both splits have the same delta value
- Positive value if split1 has a larger delta (less balanced) than split2

By sorting in ascending order of delta values, the most balanced splits (lowest deltas) appear first in the array, allowing subsequent split selection algorithms to examine the best candidates first.

## Parameters / Member Variables
- `arg1`: Pointer to the first SplitPoint structure to compare
- `arg2`: Pointer to the second SplitPoint structure to compare

## Dependencies
- Functions called/Symbols referenced:
  - [pg_cmp_s16](../p/pg_cmp_s16.md): PostgreSQL utility function for comparing two int16 values
  - SplitPoint: Structure type for split point data
- Called from:
  - [_bt_deltasortsplits](_bt_deltasortsplits.md): Used as qsort comparison function

## Notes and Other Information
- Follows the standard qsort comparator interface with void pointers that are cast to SplitPoint pointers
- Uses PostgreSQLs pg_cmp_s16 utility function for robust int16 comparison
- The delta values being compared represent absolute differences in space utilization
- This function is essential for the multi-strategy split point selection algorithm
- The sort order (ascending by delta) prioritizes balanced splits over unbalanced ones
- Function is static and only used internally within the nbtsplitloc.c module