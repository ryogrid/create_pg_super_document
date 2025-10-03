# entryExecPlaceToPage

## Location
[src/backend/access/gin/ginentrypage.c:554-601](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginentrypage.c#L554-L601)

## Overview
Executes the actual tuple insertion into a GIN entry page within a critical section, including page preparation, item insertion, and WAL logging.

## Definition
```c
static void entryExecPlaceToPage(GinBtree btree, Buffer buf, GinBtreeStack *stack,
                                void *insertPayload, BlockNumber updateblkno,
                                void *ptp_workspace)
```

## Detailed Description
entryExecPlaceToPage performs the actual insertion operation after entryBeginPlaceToPage has determined that the insertion can proceed without splitting. This function operates within a critical section and handles:

1. **Page Preparation**: Calls entryPreparePage to handle any necessary cleanup (tuple deletion, downlink updates) before insertion.

2. **Tuple Insertion**: Uses PageAddItem to insert the new entry tuple at the specified offset on the page.

3. **Error Handling**: Verifies the insertion succeeded at the expected offset, throwing an error if placement fails.

4. **Buffer Management**: Marks the buffer as dirty to ensure the changes are written to disk.

5. **WAL Logging**: When WAL is required and not during index build, creates comprehensive transaction log records including the insertion data and tuple contents for crash recovery.

The function ensures atomic insertion with proper logging for durability and consistency.

## Parameters / Member Variables
- `btree`: GinBtree structure containing B-tree context and index relation information
- `buf`: Buffer containing the target page for insertion (registered in XLOG slot 0)
- `stack`: GinBtreeStack indicating the insertion position within the page
- `insertPayload`: Generic pointer to insertion data, cast to GinBtreeEntryInsertData internally
- `updateblkno`: Block number for updating downlinks in internal nodes (when child splits occur)
- `ptp_workspace`: Workspace data passed from beginPlaceToPage (unused in this function)

## Dependencies
- Functions called/Symbols referenced:
  - [entryPreparePage](entryPreparePage.md)
  - [BufferGetPage](../B/BufferGetPage.md)
  - PageAddItem
  - IndexTupleSize
  - [MarkBufferDirty](../M/MarkBufferDirty.md)
  - RelationNeedsWAL
  - [XLogRegisterBuffer](../X/XLogRegisterBuffer.md)
  - [XLogRegisterBufData](../X/XLogRegisterBufData.md)
  - RelationGetRelationName (for error reporting)
- Called from (representative examples):
  - [ginPrepareEntryScan](../g/ginPrepareEntryScan.md)

## Notes and Other Information
- This function executes within a critical section where no errors should occur
- WAL logging uses a static ginxlogInsertEntry structure to avoid memory allocation within the critical section
- The function registers the buffer and tuple data for transaction logging when WAL is enabled
- Part of the three-phase insertion pattern ensuring atomic operations in PostgreSQL indexes
- Error reporting includes the relation name for better debugging context
- The function handles both leaf and internal page insertions through the updateblkno parameter

## Simplified Source

```c
static void
entryExecPlaceToPage(GinBtree btree, Buffer buf, GinBtreeStack *stack,
                     void *insertPayload, BlockNumber updateblkno,
                     void *ptp_workspace)
{
    GinBtreeEntryInsertData *insertData = insertPayload;
    Page page = BufferGetPage(buf);
    OffsetNumber off = stack->off;
    OffsetNumber placed;

    // Prepare page (delete old tuple, update downlinks if needed)
    entryPreparePage(btree, page, off, insertData, updateblkno);

    // Insert the new tuple
    placed = PageAddItem(page,
                         (Item) insertData->entry,
                         IndexTupleSize(insertData->entry),
                         off, false, false);

    // Verify insertion succeeded at expected offset
    if (placed != off)
        elog(ERROR, "failed to add item to index page in \"%s\"",
             RelationGetRelationName(btree->index));

    // Mark buffer as modified
    MarkBufferDirty(buf);

    // Log to WAL if needed
    if (RelationNeedsWAL(btree->index) && !btree->isBuild) {
        static ginxlogInsertEntry data;

        data.isDelete = insertData->isDelete;
        data.offset = off;

        XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
        XLogRegisterBufData(0, (char *) &data,
                            offsetof(ginxlogInsertEntry, tuple));
        XLogRegisterBufData(0, (char *) insertData->entry,
                            IndexTupleSize(insertData->entry));
    }
}
```