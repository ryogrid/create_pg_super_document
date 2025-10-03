# brin_doinsert

## Location
[src/backend/access/brin/brin_pageops.c:342-474](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_pageops.c#L342-L474)

## Overview
Inserts a new BRIN tuple into the index relation, updating the revmap to associate the heap block range with the inserted tuple and ensuring proper WAL logging.

## Definition

```c
OffsetNumber
brin_doinsert(Relation idxrel, BlockNumber pagesPerRange,
			  BrinRevmap *revmap, Buffer *buffer, BlockNumber heapBlk,
			  BrinTuple *tup, Size itemsz)
```
## Detailed Description
The  function handles the complete process of inserting a BRIN tuple into the index. It manages buffer allocation, space validation, page initialization for newly extended pages, and maintains the critical revmap structure that maps heap block ranges to their corresponding index tuples.

The function first validates that the tuple size is within acceptable limits, then ensures the revmap is extended to cover the target heap block. It attempts to use the provided buffer if it has sufficient space, otherwise obtains a new buffer through . The insertion is performed atomically within a critical section, with proper WAL logging to ensure crash recovery.

Key responsibilities include:
- Tuple size validation and error handling
- Buffer management and space checking
- Page initialization for extended relations
- Revmap updates to maintain heap-to-index mappings
- WAL logging for crash recovery
- Free space map maintenance

## Parameters / Member Variables
- : The BRIN index relation where the tuple will be inserted
- : Number of heap pages covered by each BRIN tuple
- : Reverse mapping structure for heap block to index tuple lookups
- : Pointer to buffer that may be used for insertion (may be updated)
- : Starting heap block number for the range this tuple summarizes
- : Pointer to the BRIN tuple to be inserted
- : Size of the tuple being inserted (must be MAXALIGN'd)

## Dependencies
- Functions called/Symbols referenced:
  - : Extends revmap to cover the required heap block
  - : Obtains a suitable buffer for tuple insertion
  - : Initializes newly allocated pages
  - : Adds the tuple to the page
  - : Updates revmap with new tuple location
  - : WAL logging for crash recovery
  - : Updates free space map
- Called from (representative examples):
  - : During range summarization operations
  - : When creating and inserting new summary tuples
  - : During parallel BRIN operations
  - : When filling gaps in BRIN coverage

## Notes and Other Information
- Returns the offset number where the tuple was inserted, or  on error
- The function modifies the buffer pointer if a new buffer is obtained
- Includes comprehensive error checking for oversized tuples
- Uses critical sections to ensure atomicity of multi-buffer operations
- Handles both regular insertions and insertions that require page extension
- Updates both the main index page and the revmap page atomically
- Includes debug logging to track insertion operations
- Properly manages buffer locks to prevent race conditions
- The caller retains responsibility for the buffer after insertion

## Simplified Source

```c
OffsetNumber brin_doinsert(Relation idxrel, BlockNumber pagesPerRange,
                          BrinRevmap *revmap, Buffer *buffer, BlockNumber heapBlk,
                          BrinTuple *tup, Size itemsz) {

    // Validate tuple size
    if (itemsz > BrinMaxItemSize) {
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                       errmsg("index row size %zu exceeds maximum %zu for index \"%s\"",
                             itemsz, BrinMaxItemSize, RelationGetRelationName(idxrel))));
    }

    // Ensure revmap covers the heap block
    brinRevmapExtend(revmap, heapBlk);

    bool extended = false;

    // Check if provided buffer has enough space
    if (BufferIsValid(*buffer)) {
        LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);
        if (br_page_get_freespace(BufferGetPage(*buffer)) < itemsz) {
            // Not enough space, need a new buffer
            UnlockReleaseBuffer(*buffer);
            *buffer = InvalidBuffer;
        }
    }

    // Get a suitable buffer if we don't have one
    if (!BufferIsValid(*buffer)) {
        do {
            *buffer = brin_getinsertbuffer(idxrel, InvalidBuffer, itemsz, &extended);
        } while (!BufferIsValid(*buffer));
    }

    // Lock revmap page for atomic update
    Buffer revmapbuf = brinLockRevmapPageForUpdate(revmap, heapBlk);

    Page page = BufferGetPage(*buffer);
    BlockNumber blk = BufferGetBlockNumber(*buffer);

    START_CRIT_SECTION();

    // Initialize page if it was newly extended
    if (extended) {
        brin_page_init(page, BRIN_PAGETYPE_REGULAR);
    }

    // Insert the tuple
    OffsetNumber off = PageAddItem(page, (Item) tup, itemsz, InvalidOffsetNumber,
                                  false, false);
    if (off == InvalidOffsetNumber) {
        elog(ERROR, "failed to add BRIN tuple to new page");
    }
    MarkBufferDirty(*buffer);

    // Update revmap to point to the new tuple
    ItemPointerData tid;
    ItemPointerSet(&tid, blk, off);
    brinSetHeapBlockItemptr(revmapbuf, pagesPerRange, heapBlk, tid);
    MarkBufferDirty(revmapbuf);

    // WAL logging
    if (RelationNeedsWAL(idxrel)) {
        xl_brin_insert xlrec;
        uint8 info = XLOG_BRIN_INSERT | (extended ? XLOG_BRIN_INIT_PAGE : 0);

        xlrec.heapBlk = heapBlk;
        xlrec.pagesPerRange = pagesPerRange;
        xlrec.offnum = off;

        XLogBeginInsert();
        XLogRegisterData((char *) &xlrec, SizeOfBrinInsert);
        XLogRegisterBuffer(0, *buffer, REGBUF_STANDARD | (extended ? REGBUF_WILL_INIT : 0));
        XLogRegisterBufData(0, (char *) tup, itemsz);
        XLogRegisterBuffer(1, revmapbuf, 0);

        XLogRecPtr recptr = XLogInsert(RM_BRIN_ID, info);
        PageSetLSN(page, recptr);
        PageSetLSN(BufferGetPage(revmapbuf), recptr);
    }

    END_CRIT_SECTION();

    // Release locks
    LockBuffer(*buffer, BUFFER_LOCK_UNLOCK);
    LockBuffer(revmapbuf, BUFFER_LOCK_UNLOCK);

    // Update free space map for newly extended pages
    if (extended) {
        Size freespace = br_page_get_freespace(page);
        RecordPageWithFreeSpace(idxrel, blk, freespace);
        FreeSpaceMapVacuumRange(idxrel, blk, blk + 1);
    }

    return off;
}
```