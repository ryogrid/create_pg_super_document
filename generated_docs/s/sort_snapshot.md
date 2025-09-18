# sort_snapshot

## Location
[src/backend/utils/adt/xid8funcs.c:173-186](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xid8funcs.c#L173-L186)

## Overview
Sorts the transaction IDs in a snapshot structure and removes any duplicate entries to ensure efficient searching and consistent on-disk representation.

## Definition
```c
static void sort_snapshot(pg_snapshot *snap)
```

## Detailed Description
This function sorts the array of transaction IDs (xip) within a pg_snapshot structure using the qsort() function with the cmp_fxid comparator. After sorting, it removes duplicate transaction IDs using qunique(). The sorting is performed unconditionally for consistency of on-disk representation, even when binary search functionality might not be used later.

The function only performs sorting when there are more than one transaction IDs in the snapshot (nxip > 1). After removing duplicates, the nxip count is updated to reflect the actual number of unique transaction IDs remaining in the array.

## Parameters / Member Variables
- `snap`: Pointer to a pg_snapshot structure containing the transaction ID array to be sorted and deduplicated

## Dependencies
- Functions called/Symbols referenced:
  - qsort (C library function)
  - [qunique](../q/qunique.md) (PostgreSQL utility function)
  - [cmp_fxid](../c/cmp_fxid.md) (comparison function)
  - FullTransactionId (type)
  - [pg_snapshot](../p/pg_snapshot.md) (type)
- Called from (representative examples):
  - [pg_current_snapshot](../p/pg_current_snapshot.md)

## Notes and Other Information
- This is a static function used internally within the xid8funcs.c module
- Maintains sorted order for efficient binary search operations in other functions
- Ensures consistent on-disk representation regardless of whether bsearch will be used
- The qunique function removes duplicates while preserving the sorted order
- Critical for snapshot processing and transaction visibility determination