# heap_inplace_update_and_unlock

## Location
[src/backend/access/heap/heapam.c:6432-6508](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L6432-L6508)

## Overview
Performs the core inplace update operation by copying new data into the existing tuple and releasing locks.

## Definition
```c
void heap_inplace_update_and_unlock(Relation relation, HeapTuple oldtup, HeapTuple tuple, Buffer buffer)
```

## Detailed Description
This function executes the actual inplace update by copying the new tuple data directly into the existing tuple's location in the buffer page. It enforces strict size constraints - the new tuple must have exactly the same total length and header offset as the old tuple, ensuring the tuple structure remains unchanged.

The operation sequence:
1. Validates that new and old tuples have identical sizes and header offsets
2. Performs atomic memcpy of the new data over the old tuple's data area
3. Logs the operation via WAL for crash recovery
4. Releases all locks by calling heap_inplace_unlock
5. Sends cache invalidation messages to notify other processes

Critical constraints:
- Tuple cannot change size (total length must be identical)
- Header fields and null bitmap cannot change
- Only the data portion after t_hoff is modified
- Must be called within the context of successful heap_inplace_lock

## Parameters / Member Variables
- `relation`: The heap relation being updated
- `oldtup`: The existing tuple being modified inplace
- `tuple`: The new tuple containing the updated data
- `buffer`: Buffer containing the page with the tuple (must be exclusively locked)

## Dependencies
- Functions called/Symbols referenced:
  - [ItemPointerEquals](../I/ItemPointerEquals.md)
  - START_CRIT_SECTION/END_CRIT_SECTION
  - memcpy
  - MarkBufferDirty
  - RelationNeedsWAL
  - [ItemPointerGetOffsetNumber](../I/ItemPointerGetOffsetNumber.md)
  - [XLogBeginInsert](../X/XLogBeginInsert.md)
  - [XLogRegisterData](../X/XLogRegisterData.md)
  - [XLogRegisterBuffer](../X/XLogRegisterBuffer.md)
  - [XLogRegisterBufData](../X/XLogRegisterBufData.md)
  - [XLogInsert](../X/XLogInsert.md)
  - [PageSetLSN](../P/PageSetLSN.md)
  - [BufferGetPage](../B/BufferGetPage.md)
  - [heap_inplace_unlock](heap_inplace_unlock.md)
  - IsBootstrapProcessingMode
  - [CacheInvalidateHeapTuple](../C/CacheInvalidateHeapTuple.md)
- Called from (representative examples):
  - [systable_inplace_update_finish](../s/systable_inplace_update_finish.md)
  - HeapScanIsValid (indirect reference)

## Notes and Other Information
- Generates XLOG_HEAP_INPLACE WAL record for crash recovery
- Does not support operations that change catcache lookup keys
- Does not update indexes (consistent with inplace update philosophy)
- Contains a race condition comment regarding datfrozenxid vs relfrozenxid during crashes
- Cache invalidation can be discarded on ROLLBACK (noted in inplace-inval.spec test)
- Must be preceded by successful heap_inplace_lock call
- The memcpy operation directly overwrites existing tuple data without intermediate storage
- Designed specifically for system catalog updates where tuple size remains constant