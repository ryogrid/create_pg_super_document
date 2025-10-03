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

## Simplified Source

```c
// Simplified version of shiftList
static void shiftList(Relation index, Buffer metabuffer, BlockNumber newHead,
                     bool fill_fsm, IndexBulkDeleteResult *stats)
{
    Page metapage = BufferGetPage(metabuffer);
    GinMetaPageData *metadata = GinPageGetMeta(metapage);
    BlockNumber blknoToDelete = metadata->head;

    do {
        ginxlogDeleteListPages data;
        Buffer buffers[GIN_NDELETE_AT_ONCE];
        BlockNumber freespace[GIN_NDELETE_AT_ONCE];
        int64 nDeletedHeapTuples = 0;
        int i;

        // Collect batch of pages to delete
        data.ndeleted = 0;
        while (data.ndeleted < GIN_NDELETE_AT_ONCE && blknoToDelete != newHead) {
            freespace[data.ndeleted] = blknoToDelete;
            buffers[data.ndeleted] = ReadBuffer(index, blknoToDelete);
            LockBuffer(buffers[data.ndeleted], GIN_EXCLUSIVE);

            Page page = BufferGetPage(buffers[data.ndeleted]);
            nDeletedHeapTuples += GinPageGetOpaque(page)->maxoff;
            blknoToDelete = GinPageGetOpaque(page)->rightlink;
            data.ndeleted++;
        }

        if (stats)
            stats->pages_deleted += data.ndeleted;

        // Prepare for large WAL record
        if (RelationNeedsWAL(index))
            XLogEnsureRecordSpace(data.ndeleted, 0);

        START_CRIT_SECTION();

        // Update metadata
        metadata->head = blknoToDelete;
        metadata->nPendingPages -= data.ndeleted;
        metadata->nPendingHeapTuples -= nDeletedHeapTuples;

        if (blknoToDelete == InvalidBlockNumber) {
            metadata->tail = InvalidBlockNumber;
            metadata->tailFreeSize = 0;
            metadata->nPendingPages = 0;
            metadata->nPendingHeapTuples = 0;
        }

        // Mark pages as deleted
        for (i = 0; i < data.ndeleted; i++) {
            Page page = BufferGetPage(buffers[i]);
            GinPageGetOpaque(page)->flags = GIN_DELETED;
            MarkBufferDirty(buffers[i]);
        }

        MarkBufferDirty(metabuffer);

        // WAL logging
        if (RelationNeedsWAL(index)) {
            memcpy(&data.metadata, metadata, sizeof(GinMetaPageData));
            XLogBeginInsert();
            XLogRegisterBuffer(0, metabuffer, REGBUF_WILL_INIT | REGBUF_STANDARD);
            for (i = 0; i < data.ndeleted; i++)
                XLogRegisterBuffer(i + 1, buffers[i], REGBUF_WILL_INIT);
            XLogRegisterData((char *) &data, sizeof(ginxlogDeleteListPages));

            XLogRecPtr recptr = XLogInsert(RM_GIN_ID, XLOG_GIN_DELETE_LISTPAGE);
            PageSetLSN(metapage, recptr);
            for (i = 0; i < data.ndeleted; i++)
                PageSetLSN(BufferGetPage(buffers[i]), recptr);
        }

        // Release buffers
        for (i = 0; i < data.ndeleted; i++)
            UnlockReleaseBuffer(buffers[i]);

        END_CRIT_SECTION();

        // Record freed pages in FSM
        if (fill_fsm) {
            for (i = 0; i < data.ndeleted; i++)
                RecordFreeIndexPage(index, freespace[i]);
        }

    } while (blknoToDelete != newHead);
}
```