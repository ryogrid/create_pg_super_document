# brin_doupdate

## Location
[src/backend/access/brin/brin_pageops.c:53-322](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_pageops.c#L53-L322)

## Overview
Updates a BRIN (Block Range Index) tuple by replacing an existing tuple with a new one, handling both same-page and cross-page updates with proper WAL logging and revmap maintenance.

## Definition

```c
bool
brin_doupdate(Relation idxrel, BlockNumber pagesPerRange,
			  BrinRevmap *revmap, BlockNumber heapBlk,
			  Buffer oldbuf, OffsetNumber oldoff,
			  const BrinTuple *origtup, Size origsz,
			  const BrinTuple *newtup, Size newsz,
			  bool samepage)
```
## Detailed Description
The  function performs atomic updates of BRIN index tuples, which represent summarized information about ranges of heap blocks. The function handles two main scenarios:

1. **Same-page update**: When the new tuple fits in the same page as the original tuple, it performs an in-place replacement using .

2. **Cross-page update**: When there's insufficient space on the original page, it removes the old tuple and inserts the new tuple on a different page, updating the revmap to maintain the mapping from heap block ranges to index tuples.

The function includes comprehensive validation to detect concurrent modifications, ensures proper WAL logging for crash recovery, and manages buffer locking to maintain consistency. It also handles page extension when necessary and updates the free space map appropriately.

## Parameters / Member Variables
- `idxrel`: The BRIN index relation being updated
- `pagesPerRange`: Number of heap pages covered by each BRIN tuple
- `*revmap`: Reverse mapping structure that tracks heap block to index tuple mappings
- `heapBlk`: Starting heap block number for the range being updated
- `oldbuf`: Buffer containing the page with the original tuple
- `oldoff`: Offset number of the original tuple within the page
- `*origtup`: Pointer to the original tuple (used for validation)
- `origsz`: Size of the original tuple
- `*newtup`: Pointer to the new tuple to be inserted
- `newsz`: Size of the new tuple
- `samepage`: Boolean flag indicating whether to attempt same-page update
## Dependencies
- Functions called/Symbols referenced:
  - : Extends revmap to cover the required heap block
  - : Finds a suitable buffer for tuple insertion
  - : Checks if same-page update is possible
  - : Validates tuple equality for concurrency control
  - : Performs in-place tuple replacement
  - : Locks revmap page for atomic updates
  - : Updates revmap with new tuple location
  - : WAL logging for crash recovery
- Called from (representative examples):
  - : Main BRIN insertion function
  - : Range summarization during index maintenance
  - : Page evacuation during vacuum operations

## Notes and Other Information
- The function returns  on successful update,  if the update should be retried
- Implements proper concurrency control by validating that the original tuple hasn't been modified
- Handles page evacuation flags and ensures evacuated pages are not used for same-page updates
- Includes comprehensive WAL logging with different record types for same-page vs cross-page updates
- Manages free space map updates when new pages are allocated
- Uses critical sections to ensure atomicity of multi-buffer operations
- Validates tuple size limits and returns appropriate errors for oversized tuples

## Simplified Source

```c
bool brin_doupdate(Relation idxrel, BlockNumber pagesPerRange,
                   BrinRevmap *revmap, BlockNumber heapBlk,
                   Buffer oldbuf, OffsetNumber oldoff,
                   const BrinTuple *origtup, Size origsz,
                   const BrinTuple *newtup, Size newsz,
                   bool samepage) {

    // Validate tuple size
    if (newsz > BrinMaxItemSize) {
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                       errmsg("index row size %zu exceeds maximum %zu for index \"%s\"",
                             newsz, BrinMaxItemSize, RelationGetRelationName(idxrel))));
    }

    // Ensure revmap can handle the heap block
    brinRevmapExtend(revmap, heapBlk);

    Buffer newbuf = InvalidBuffer;
    bool extended = false;

    // Get a buffer for the new tuple if not doing same-page update
    if (!samepage) {
        newbuf = brin_getinsertbuffer(idxrel, oldbuf, newsz, &extended);
        if (!BufferIsValid(newbuf)) {
            return false;  // Retry needed
        }
        if (newbuf == oldbuf) {
            newbuf = InvalidBuffer;  // Same page after all
        }
    } else {
        LockBuffer(oldbuf, BUFFER_LOCK_EXCLUSIVE);
    }

    // Validate the original tuple hasn't changed
    Page oldpage = BufferGetPage(oldbuf);
    ItemId oldlp = PageGetItemId(oldpage, oldoff);

    if (!BRIN_IS_REGULAR_PAGE(oldpage) ||
        oldoff > PageGetMaxOffsetNumber(oldpage) ||
        !ItemIdIsNormal(oldlp)) {
        // Page or tuple state changed, cleanup and retry
        LockBuffer(oldbuf, BUFFER_LOCK_UNLOCK);
        if (BufferIsValid(newbuf)) {
            if (extended) brin_initialize_empty_new_buffer(idxrel, newbuf);
            UnlockReleaseBuffer(newbuf);
        }
        return false;
    }

    BrinTuple *oldtup = (BrinTuple *) PageGetItem(oldpage, oldlp);
    Size oldsz = ItemIdGetLength(oldlp);

    // Check if tuple content changed
    if (!brin_tuples_equal(oldtup, oldsz, origtup, origsz)) {
        LockBuffer(oldbuf, BUFFER_LOCK_UNLOCK);
        if (BufferIsValid(newbuf)) {
            if (extended) brin_initialize_empty_new_buffer(idxrel, newbuf);
            UnlockReleaseBuffer(newbuf);
        }
        return false;
    }

    // Try same-page update if possible
    if (((BrinPageFlags(oldpage) & BRIN_EVACUATE_PAGE) == 0) &&
        brin_can_do_samepage_update(oldbuf, origsz, newsz)) {

        START_CRIT_SECTION();

        // Replace tuple in place
        if (!PageIndexTupleOverwrite(oldpage, oldoff, (Item) newtup, newsz)) {
            elog(ERROR, "failed to replace BRIN tuple");
        }
        MarkBufferDirty(oldbuf);

        // WAL logging for same-page update
        if (RelationNeedsWAL(idxrel)) {
            xl_brin_samepage_update xlrec;
            xlrec.offnum = oldoff;

            XLogBeginInsert();
            XLogRegisterData((char *) &xlrec, SizeOfBrinSamepageUpdate);
            XLogRegisterBuffer(0, oldbuf, REGBUF_STANDARD);
            XLogRegisterBufData(0, (char *) newtup, newsz);

            XLogRecPtr recptr = XLogInsert(RM_BRIN_ID, XLOG_BRIN_SAMEPAGE_UPDATE);
            PageSetLSN(oldpage, recptr);
        }

        END_CRIT_SECTION();
        LockBuffer(oldbuf, BUFFER_LOCK_UNLOCK);

        if (BufferIsValid(newbuf)) {
            if (extended) brin_initialize_empty_new_buffer(idxrel, newbuf);
            UnlockReleaseBuffer(newbuf);
        }
        return true;
    }

    // Cross-page update: move tuple to new page
    if (newbuf == InvalidBuffer) {
        LockBuffer(oldbuf, BUFFER_LOCK_UNLOCK);
        return false;  // No space and caller thought there was
    }

    Page newpage = BufferGetPage(newbuf);
    Buffer revmapbuf = brinLockRevmapPageForUpdate(revmap, heapBlk);

    START_CRIT_SECTION();

    // Initialize new page if needed
    if (extended) {
        brin_page_init(newpage, BRIN_PAGETYPE_REGULAR);
    }

    // Remove old tuple and add new one
    PageIndexTupleDeleteNoCompact(oldpage, oldoff);
    OffsetNumber newoff = PageAddItem(newpage, (Item) newtup, newsz,
                                     InvalidOffsetNumber, false, false);
    if (newoff == InvalidOffsetNumber) {
        elog(ERROR, "failed to add BRIN tuple to new page");
    }

    // Mark buffers dirty and update revmap
    MarkBufferDirty(oldbuf);
    MarkBufferDirty(newbuf);

    ItemPointerData newtid;
    ItemPointerSet(&newtid, BufferGetBlockNumber(newbuf), newoff);
    brinSetHeapBlockItemptr(revmapbuf, pagesPerRange, heapBlk, newtid);
    MarkBufferDirty(revmapbuf);

    // WAL logging for cross-page update
    if (RelationNeedsWAL(idxrel)) {
        xl_brin_update xlrec;
        uint8 info = XLOG_BRIN_UPDATE | (extended ? XLOG_BRIN_INIT_PAGE : 0);

        xlrec.insert.offnum = newoff;
        xlrec.insert.heapBlk = heapBlk;
        xlrec.insert.pagesPerRange = pagesPerRange;
        xlrec.oldOffnum = oldoff;

        XLogBeginInsert();
        XLogRegisterData((char *) &xlrec, SizeOfBrinUpdate);
        XLogRegisterBuffer(0, newbuf, REGBUF_STANDARD | (extended ? REGBUF_WILL_INIT : 0));
        XLogRegisterBufData(0, (char *) newtup, newsz);
        XLogRegisterBuffer(1, revmapbuf, 0);
        XLogRegisterBuffer(2, oldbuf, REGBUF_STANDARD);

        XLogRecPtr recptr = XLogInsert(RM_BRIN_ID, info);
        PageSetLSN(oldpage, recptr);
        PageSetLSN(newpage, recptr);
        PageSetLSN(BufferGetPage(revmapbuf), recptr);
    }

    END_CRIT_SECTION();

    // Release locks and update free space map
    LockBuffer(revmapbuf, BUFFER_LOCK_UNLOCK);
    LockBuffer(oldbuf, BUFFER_LOCK_UNLOCK);
    UnlockReleaseBuffer(newbuf);

    if (extended) {
        Size freespace = br_page_get_freespace(newpage);
        RecordPageWithFreeSpace(idxrel, BufferGetBlockNumber(newbuf), freespace);
        FreeSpaceMapVacuumRange(idxrel, BufferGetBlockNumber(newbuf),
                               BufferGetBlockNumber(newbuf) + 1);
    }

    return true;
}
```