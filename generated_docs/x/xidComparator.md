# xidComparator

## Location
src/backend/utils/adt/xid.c: 139 - 155

## Overview
The `xidComparator` function is a qsort comparison function specifically designed for sorting arrays of transaction IDs (XIDs) in PostgreSQL.

## Definition
```c
int xidComparator(const void *arg1, const void *arg2)
```

## Detailed Description
This function provides a comparison mechanism for sorting transaction IDs using standard C library sorting functions like qsort. Unlike PostgreSQL's wraparound-aware transaction ID comparison, this function uses simple unsigned 32-bit integer comparison to maintain the triangle inequality property required by sorting algorithms. The function extracts two TransactionId values from void pointers and returns their numerical comparison result. This approach ensures consistent and mathematically sound sorting behavior, even though it doesn't account for transaction ID wraparound semantics.

## Parameters / Member Variables
- `arg1`: Pointer to the first TransactionId to compare
- `arg2`: Pointer to the second TransactionId to compare

## Dependencies
- Functions called/Symbols referenced:
  - pg_cmp_u32 (function for comparing unsigned 32-bit integers)
- Called from (representative examples):
  - TransactionIdInArray (in heapam_visibility.c and reorderbuffer.c)
  - SerializeTransactionState (in xact.c)
  - ReorderBufferCopySnap (in reorderbuffer.c)
  - ReorderBufferGetCatalogChangesXacts (in reorderbuffer.c)
  - SnapBuildBuildSnapshot (in snapbuild.c)
  - SnapBuildInitialSnapshot (in snapbuild.c)
  - SnapBuildXidHasCatalogChanges (in snapbuild.c)

## Notes and Other Information
- Does not use wraparound comparison because it would violate the triangle inequality required by sorting algorithms
- Uses simple numerical comparison suitable for qsort and other standard sorting functions
- Commonly used in logical replication and snapshot building contexts
- Returns negative, zero, or positive values following standard comparator conventions
- Critical for maintaining consistent transaction ID ordering in arrays
- Located in src/backend/utils/adt/xid.c:139-155