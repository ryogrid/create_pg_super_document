# shiftList

## Location
[src/backend/access/gin/ginfast.c:554-674](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginfast.c#L554-L674)

## Overview
A static function that deletes pending list pages up to a specified head page, updating metadata and optionally recording freed pages in the free space map.

## Definition

```c
static void
shiftList(Relation index, Buffer metabuffer, BlockNumber newHead,
		  bool fill_fsm, IndexBulkDeleteResult *stats)
```
## Detailed Description
This function is responsible for removing processed pages from GIN's pending list during cleanup operations. It operates by traversing the linked list of pending pages from the current head up to (but not including) the newHead page, deleting pages in batches of GIN_NDELETE_AT_ONCE for efficiency. The function maintains metadata consistency by updating page counts and heap tuple counts, handles WAL logging for crash recovery, marks deleted pages with GIN_DELETED flag, and optionally records freed pages in the free space map for reuse. When newHead is InvalidBlockNumber, it deletes the entire pending list and resets all metadata counters to zero.

## Parameters / Member Variables
- `index`: The GIN index relation being cleaned up
- `metabuffer`: Buffer containing the index metapage (must be pinned and exclusively locked)
- `newHead`: Block number of the new head page, or InvalidBlockNumber to delete entire list
- `fill_fsm`: Boolean indicating whether to record freed pages in the free space map
- `stats`: Pointer to IndexBulkDeleteResult for tracking deletion statistics (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - GinPageGetMeta
  - [ReadBuffer](../R/ReadBuffer.md)
  - [LockBuffer](../L/LockBuffer.md)
  - GinPageIsDeleted
  - GinPageGetOpaque
  - RelationNeedsWAL
  - [XLogEnsureRecordSpace](../X/XLogEnsureRecordSpace.md)
  - [XLogBeginInsert](../X/XLogBeginInsert.md)
  - [XLogRegisterBuffer](../X/XLogRegisterBuffer.md)
  - [XLogRegisterData](../X/XLogRegisterData.md)
  - [XLogInsert](../X/XLogInsert.md)
  - [RecordFreeIndexPage](../R/RecordFreeIndexPage.md)
- Called from (representative examples):
  - [ginInsertCleanup](../g/ginInsertCleanup.md)

## Notes and Other Information
- Requires metapage to be pinned and exclusively locked throughout operation
- Processes pages in batches of GIN_NDELETE_AT_ONCE to limit resource usage
- Uses XLogEnsureRecordSpace before critical section due to large number of pages
- Maintains accurate counters for nPendingPages and nPendingHeapTuples
- Sets pd_lower on metapage to prevent metadata loss during WAL compression
- Marks deleted pages with GIN_DELETED flag rather than immediately freeing them
- Operates within START_CRIT_SECTION/END_CRIT_SECTION for atomicity
- Part of GIN's cleanup mechanism for maintaining pending list efficiency
- Handles complete list deletion when newHead == InvalidBlockNumber
- Updates bulk delete statistics when stats parameter is provided