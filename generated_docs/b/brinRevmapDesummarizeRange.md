# brinRevmapDesummarizeRange

## Location
[src/backend/access/brin/brin_revmap.c:323-441](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_revmap.c#L323-L441)

## Overview
Deletes an index tuple from a BRIN index, marking a page range as unsummarized and removing its summary information from both the reverse map and the regular index pages.

## Definition

```c
struct;
```
## Detailed Description
This function removes the summary tuple for a given heap block range by:
1. Initializing the reverse map and locating the relevant revmap page
2. Obtaining an exclusive lock on the revmap page for the specified heap block
3. Reading the ItemPointer from the revmap that points to the actual summary tuple
4. Locating and locking the regular index page containing the summary tuple
5. Validating the tuple exists and the pages are in the expected state
6. Removing the tuple from both the regular page and clearing the revmap entry
7. Recording the operation in WAL if necessary

The function handles various edge cases including missing tuples, concurrent page type changes, and leftover placeholder tuples from crashed operations. It operates within a critical section to ensure atomicity.

## Parameters / Member Variables
- : The BRIN index relation from which to remove the summary tuple
- : The heap block number whose range should be desummarized

## Dependencies
- Functions called/Symbols referenced:
  - [brinRevmapInitialize](brinRevmapInitialize.md)
  - [revmap_get_blkno](../r/revmap_get_blkno.md)
  - BlockNumberIsValid
  - [brinRevmapTerminate](brinRevmapTerminate.md)
  - [brinLockRevmapPageForUpdate](brinLockRevmapPageForUpdate.md)
  - HEAPBLK_TO_REVMAP_INDEX
  - [PageGetContents](../P/PageGetContents.md)
  - [ItemPointerIsValid](../I/ItemPointerIsValid.md)
  - [ReadBuffer](../R/ReadBuffer.md)
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)
  - BRIN_IS_REGULAR_PAGE
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - ItemIdIsUsed
  - [brinSetHeapBlockItemptr](brinSetHeapBlockItemptr.md)
  - [PageIndexTupleDeleteNoCompact](../P/PageIndexTupleDeleteNoCompact.md)
  - [MarkBufferDirty](../M/MarkBufferDirty.md)
  - [XLogBeginInsert](../X/XLogBeginInsert.md)
  - [XLogRegisterData](../X/XLogRegisterData.md)
  - [XLogRegisterBuffer](../X/XLogRegisterBuffer.md)
  - [XLogInsert](../X/XLogInsert.md)
- Called from (representative examples):
  - [brin_desummarize_range](brin_desummarize_range.md)

## Notes and Other Information
- Requires ShareUpdateExclusiveLock on the index to prevent concurrent summarization
- Returns false if the caller should retry the operation (due to concurrent page changes)
- Returns true if the operation completed successfully or if the range was already unsummarized
- Handles placeholder tuples from aborted summarization operations by removing them silently
- Records the desummarization in WAL for crash recovery when WAL logging is enabled
- Uses critical sections to ensure the operation is atomic across both revmap and regular pages
- Validates index consistency and reports INDEX_CORRUPTED errors for invalid states

## Simplified Source

```c
bool brinRevmapDesummarizeRange(Relation idxrel, BlockNumber heapBlk)
{
    // Initialize revmap and find the revmap page for this heap block
    BlockNumber pagesPerRange;
    BrinRevmap *revmap = brinRevmapInitialize(idxrel, &pagesPerRange);

    BlockNumber revmapBlk = revmap_get_blkno(revmap, heapBlk);
    if (!BlockNumberIsValid(revmapBlk)) {
        brinRevmapTerminate(revmap);
        return true;  // Range not summarized, nothing to do
    }

    // Lock revmap page and get pointer to index tuple
    Buffer revmapBuf = brinLockRevmapPageForUpdate(revmap, heapBlk);
    Page revmapPg = BufferGetPage(revmapBuf);
    OffsetNumber revmapOffset = HEAPBLK_TO_REVMAP_INDEX(revmap->rm_pagesPerRange, heapBlk);

    RevmapContents *contents = (RevmapContents *) PageGetContents(revmapPg);
    ItemPointerData *iptr = contents->rm_tids + revmapOffset;

    if (!ItemPointerIsValid(iptr)) {
        LockBuffer(revmapBuf, BUFFER_LOCK_UNLOCK);
        brinRevmapTerminate(revmap);
        return true;  // No index tuple, nothing to do
    }

    // Read and lock the regular index page containing the tuple
    Buffer regBuf = ReadBuffer(idxrel, ItemPointerGetBlockNumber(iptr));
    LockBuffer(regBuf, BUFFER_LOCK_EXCLUSIVE);
    Page regPg = BufferGetPage(regBuf);

    // Validate page is still a regular page
    if (!BRIN_IS_REGULAR_PAGE(regPg)) {
        LockBuffer(revmapBuf, BUFFER_LOCK_UNLOCK);
        LockBuffer(regBuf, BUFFER_LOCK_UNLOCK);
        brinRevmapTerminate(revmap);
        return false;  // Caller should retry
    }

    // Validate tuple offset and existence
    OffsetNumber regOffset = ItemPointerGetOffsetNumber(iptr);
    if (regOffset > PageGetMaxOffsetNumber(regPg))
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                       errmsg("corrupted BRIN index: inconsistent range map")));

    ItemId lp = PageGetItemId(regPg, regOffset);
    if (!ItemIdIsUsed(lp))
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                       errmsg("corrupted BRIN index: inconsistent range map")));

    START_CRIT_SECTION();

    // Clear the revmap entry and delete the tuple
    ItemPointerData invalidIptr;
    ItemPointerSetInvalid(&invalidIptr);
    brinSetHeapBlockItemptr(revmapBuf, revmap->rm_pagesPerRange, heapBlk, invalidIptr);
    PageIndexTupleDeleteNoCompact(regPg, regOffset);

    // Mark pages dirty
    MarkBufferDirty(regBuf);
    MarkBufferDirty(revmapBuf);

    // WAL logging if needed
    if (RelationNeedsWAL(idxrel)) {
        xl_brin_desummarize xlrec;
        xlrec.pagesPerRange = revmap->rm_pagesPerRange;
        xlrec.heapBlk = heapBlk;
        xlrec.regOffset = regOffset;

        XLogBeginInsert();
        XLogRegisterData((char *) &xlrec, SizeOfBrinDesummarize);
        XLogRegisterBuffer(0, revmapBuf, 0);
        XLogRegisterBuffer(1, regBuf, REGBUF_STANDARD);
        XLogRecPtr recptr = XLogInsert(RM_BRIN_ID, XLOG_BRIN_DESUMMARIZE);

        PageSetLSN(revmapPg, recptr);
        PageSetLSN(regPg, recptr);
    }

    END_CRIT_SECTION();

    // Clean up
    UnlockReleaseBuffer(regBuf);
    LockBuffer(revmapBuf, BUFFER_LOCK_UNLOCK);
    brinRevmapTerminate(revmap);

    return true;
}
```