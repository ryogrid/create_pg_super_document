# log_heap_visible

## Location
[src/backend/access/heap/heapam.c:8782-8815](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L8782-L8815)

## Overview
Performs XLogInsert for a heap-visible operation, generating a write-ahead log (WAL) record when a heap page is marked as all-visible in the visibility map.

## Definition

```c
XLogRecPtr
log_heap_visible(Relation rel, Buffer heap_buffer, Buffer vm_buffer,
				 TransactionId snapshotConflictHorizon, uint8 vmflags)
```
## Detailed Description
The  function creates a WAL record for marking a heap page as all-visible. This is a critical operation for PostgreSQL's MVCC (Multi-Version Concurrency Control) system and vacuum operations. The function registers both the visibility map buffer and heap buffer for WAL logging, with optimizations to avoid full-page images when checksums or wal_log_hints are disabled. The resulting WAL record allows for proper recovery and replication of visibility map changes.

The function handles logical decoding accessibility by setting appropriate flags for catalog relations. It optimizes WAL record size by conditionally including full-page images based on system configuration.

## Parameters / Member Variables
- : The relation (table) containing the page being marked all-visible
- : Buffer containing the heap page being marked all-visible (must be valid and already modified)
- : Buffer containing the corresponding visibility map block (must be valid and already modified)  
- : The largest xmin on the page being marked all-visible, used by REDO routine to generate recovery conflicts
- : Visibility map flags indicating the type of visibility being set

## Dependencies
- Functions called/Symbols referenced:
  - [xl_heap_visible](../x/xl_heap_visible.md) (WAL record structure)
  - RelationIsAccessibleInLogicalDecoding
  - [XLogBeginInsert](../X/XLogBeginInsert.md)
  - [XLogRegisterData](../X/XLogRegisterData.md)
  - [XLogRegisterBuffer](../X/XLogRegisterBuffer.md)
  - [XLogInsert](../X/XLogInsert.md)
  - XLogHintBitIsNeeded
  - VISIBILITYMAP_XLOG_CATALOG_REL
  - REGBUF_STANDARD
  - REGBUF_NO_IMAGE
  - XLOG_HEAP2_VISIBLE
- Called from:
  - [visibilitymap_set](../v/visibilitymap_set.md)

## Notes and Other Information
- Both heap_buffer and vm_buffer must be valid and already modified/dirtied before calling this function
- The function returns an XLogRecPtr that represents the LSN of the inserted WAL record
- When checksums or wal_log_hints are disabled, the function optimizes by not including a full-page image of the heap buffer
- For catalog relations accessible in logical decoding, additional flags are set to ensure proper replication
- The snapshotConflictHorizon is crucial for generating proper recovery conflicts during WAL replay
- This function is part of PostgreSQL's visibility map infrastructure, which tracks which pages contain only visible tuples to optimize vacuum operations