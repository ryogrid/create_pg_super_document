# log_heap_update

## Location
[src/backend/access/heap/heapam.c:8816-9037](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L8816-L9037)

## Overview
Performs XLogInsert for a heap-update operation, creating comprehensive WAL records for tuple updates with optimizations for space efficiency and logical decoding support.

## Definition

```c
static XLogRecPtr
log_heap_update(Relation reln, Buffer oldbuf,
				Buffer newbuf, HeapTuple oldtup, HeapTuple newtup,
				HeapTuple old_key_tuple,
				bool all_visible_cleared, bool new_all_visible_cleared)
```
## Detailed Description
The  function creates WAL records for heap tuple update operations. It implements sophisticated optimizations to minimize WAL volume by detecting common prefixes and suffixes between old and new tuple versions when they reside on the same page. The function handles both regular updates and HOT (Heap-Only Tuple) updates, supports logical decoding requirements, and manages visibility map clearing flags.

Key optimizations include:
- Prefix/suffix compression when old and new tuples are on the same page
- Conditional full-page image generation based on buffer backup needs
- Special handling for logical replication requiring complete tuple data
- Page initialization detection for new pages with single tuples

## Parameters / Member Variables
- : The relation being updated
- : Buffer containing the old tuple's page
- : Buffer containing the new tuple's page  
- : The old tuple being updated
- : The new tuple version
- : The old key tuple for replica identity (nullable)
- : Whether the old page's all-visible flag was cleared
- : Whether the new page's all-visible flag was cleared

## Dependencies
- Functions called/Symbols referenced:
  - [xl_heap_update](../x/xl_heap_update.md) (WAL record structure)
  - [xl_heap_header](../x/xl_heap_header.md) (tuple header structure)
  - RelationIsLogicallyLogged
  - RelationNeedsWAL
  - HeapTupleIsHeapOnly
  - [XLogBeginInsert](../X/XLogBeginInsert.md)
  - [XLogRegisterBuffer](../X/XLogRegisterBuffer.md)
  - [XLogRegisterData](../X/XLogRegisterData.md)
  - [XLogRegisterBufData](../X/XLogRegisterBufData.md)
  - [XLogInsert](../X/XLogInsert.md)
  - [XLogCheckBufferNeedsBackup](../X/XLogCheckBufferNeedsBackup.md)
  - [XLogSetRecordFlags](../X/XLogSetRecordFlags.md)
  - [ItemPointerGetOffsetNumber](../I/ItemPointerGetOffsetNumber.md)
  - HeapTupleHeaderGetRawXmax
  - [compute_infobits](../c/compute_infobits.md)
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - XLOG_HEAP_UPDATE/XLOG_HEAP_HOT_UPDATE
  - Various XLH_UPDATE_* flags
- Called from:
  - [heap_update](../h/heap_update.md)

## Notes and Other Information
- The function is static and only used internally within heapam.c
- Implements prefix/suffix compression that requires at least 3 bytes savings to be worthwhile
- Handles logical decoding by including complete tuple data when wal_level='logical'
- Supports replica identity by logging old key tuples for REPLICA_IDENTITY_FULL
- Automatically detects page initialization scenarios for new single-tuple pages
- Sets XLOG_INCLUDE_ORIGIN flag for origin filtering efficiency in logical replication
- The caller must ensure buffers are already modified and marked dirty before calling
- Compression optimization only works when old and new tuples are on the same page to avoid corruption propagation
- Returns XLogRecPtr representing the LSN of the inserted WAL record