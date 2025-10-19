# cmp_fxid

## Location
[src/backend/utils/adt/xid8funcs.c:153-172](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xid8funcs.c#L153-L172)

## Overview
A comparison function for FullTransactionId values, designed to be used with qsort() and bsearch() functions for sorting and searching arrays of transaction IDs.

## Definition
```c
static int cmp_fxid(const void *aa, const void *bb)
```

## Detailed Description
This function implements a standard three-way comparison for FullTransactionId values. It returns -1 if the first transaction ID precedes the second, +1 if the second precedes the first, and 0 if they are equal. The function follows the standard C library comparator function interface, making it suitable for use with qsort() and bsearch() functions to sort and search arrays of FullTransactionId values.

The comparison uses the FullTransactionIdPrecedes() function to determine the ordering relationship between transaction IDs, which properly handles transaction ID wraparound and epoch comparisons.

## Parameters / Member Variables
- `aa`: Pointer to the first FullTransactionId value (cast from const void *)
- `bb`: Pointer to the second FullTransactionId value (cast from const void *)

## Dependencies
- Functions called/Symbols referenced:
  - FullTransactionIdPrecedes
  - [FullTransactionId](../F/FullTransactionId.md) (type)
- Called from (representative examples):
  - [sort_snapshot](../s/sort_snapshot.md) (for qsort operations)
  - [is_visible_fxid](../i/is_visible_fxid.md) (for bsearch operations)

## Notes and Other Information
- This is a static function used internally within the xid8funcs.c module
- Follows the standard C library comparator function signature for use with qsort/bsearch
- Essential for maintaining sorted arrays of transaction IDs in snapshot processing
- The comparison properly handles transaction ID wraparound through the underlying FullTransactionIdPrecedes function

## Simplified Source

```c
static int cmp_fxid(const void *aa, const void *bb) {
    // Extract the FullTransactionId values from void pointers
    FullTransactionId a = *(const FullTransactionId *) aa;
    FullTransactionId b = *(const FullTransactionId *) bb;

    // Three-way comparison: a < b returns -1, a > b returns 1, a == b returns 0
    if (FullTransactionIdPrecedes(a, b))
        return -1;
    if (FullTransactionIdPrecedes(b, a))
        return 1;
    return 0;
}
```