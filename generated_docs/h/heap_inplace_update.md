# heap_inplace_update

## Location
[src/backend/access/heap/heapam.c:6523-6605](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L6523-L6605)

## Overview
A deprecated function that performs in-place updates of heap tuples without creating new tuple versions, maintained for backward compatibility with modules that haven't migrated to the newer systable_inplace_update_begin() API.

## Definition


## Detailed Description
The heap_inplace_update function modifies tuple data directly in place on the heap page without creating a new tuple version or updating indexes. This is a specialized operation that's primarily used for system catalog updates where MVCC semantics aren't required. The function is deprecated and modules should migrate to using systable_inplace_update_begin() instead.

The function performs several critical operations:
1. Validates that the operation isn't running in parallel mode
2. Reads and locks the target page containing the tuple
3. Validates the tuple location and size constraints
4. Copies new data over the existing tuple data in-place
5. Logs the operation for WAL if needed
6. Invalidates relevant cache entries

The operation requires that the new tuple data has exactly the same length as the existing data and the same header offset, ensuring no structural changes to the page layout.

## Parameters
- : The relation containing the tuple to be updated
- : The new tuple data to replace the existing tuple (must have same length and header offset)

## Dependencies
- Functions called/Symbols referenced:
  - IsInParallelMode
  - [ReadBuffer](../R/ReadBuffer.md)
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)
  - [ItemPointerGetOffsetNumber](../I/ItemPointerGetOffsetNumber.md)
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - ItemIdIsNormal
  - [PageGetItem](../P/PageGetItem.md)
  - ItemIdGetLength
  - START_CRIT_SECTION/END_CRIT_SECTION
  - RelationNeedsWAL
  - [XLogBeginInsert](../X/XLogBeginInsert.md)
  - [XLogRegisterData](../X/XLogRegisterData.md)
  - [XLogRegisterBuffer](../X/XLogRegisterBuffer.md)
  - [XLogRegisterBufData](../X/XLogRegisterBufData.md)
  - [XLogInsert](../X/XLogInsert.md)
  - [CacheInvalidateHeapTuple](../C/CacheInvalidateHeapTuple.md)
- Called from:
  - HeapScanIsValid (via header inclusion)

## Notes and Other Information
- **Deprecated**: This function exists only for backward compatibility. New code should use systable_inplace_update_begin()
- **Parallel Mode Restriction**: Cannot be used during parallel operations as it may interfere with parallel execution semantics
- **Size Constraints**: Requires exact length match between old and new tuple data including header offsets
- **No Index Updates**: Does not update any indexes, so should only be used when index consistency isn't required
- **Critical Section**: Uses critical sections to ensure atomicity of the in-place modification
- **WAL Logging**: Logs the operation as XLOG_HEAP_INPLACE when WAL is enabled
- **Cache Invalidation**: Sends cache invalidation messages for the modified tuple
- **System Catalogs**: Primarily intended for system catalog maintenance operations