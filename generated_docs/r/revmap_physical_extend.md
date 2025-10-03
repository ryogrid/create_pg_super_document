# revmap_physical_extend

## Location
[src/backend/access/brin/brin_revmap.c:522-645](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_revmap.c#L522-L645)

## Overview
Attempts to extend the BRIN reverse mapping (revmap) by one physical page, handling complex concurrency scenarios and page management for BRIN indexes.

## Definition

```c
static void
revmap_physical_extend(BrinRevmap *revmap)
```
## Detailed Description
This function is responsible for extending the BRIN reverse mapping structure by adding one new physical page. The BRIN reverse mapping is crucial for efficiently mapping heap block numbers to their corresponding index tuple locations. The function implements a careful protocol to handle concurrent operations and ensures data integrity during the extension process.

The function follows a multi-step process:
1. Locks the metapage exclusively to prevent concurrent extensions
2. Validates that cached metadata is up-to-date
3. Determines the next block number for the new revmap page
4. Either reads an existing block or extends the relation if needed
5. Handles page evacuation if the target block is already in use
6. Initializes the new page as a revmap page and updates metadata
7. Logs the operation for write-ahead logging (WAL) if required

The function is designed to be retry-safe, meaning callers are expected to retry the operation until the desired outcome is achieved, as various concurrency scenarios may prevent immediate success.

## Parameters / Member Variables
- : Pointer to the BrinRevmap structure containing the reverse mapping metadata and cached information

## Dependencies
- Functions called/Symbols referenced:
  - [LockBuffer](../L/LockBuffer.md)
  - [BufferGetPage](../B/BufferGetPage.md)
  - [PageGetContents](../P/PageGetContents.md)
  - RelationGetNumberOfBlocks
  - [ReadBuffer](../R/ReadBuffer.md)
  - [ExtendBufferedRel](../E/ExtendBufferedRel.md)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)
  - [PageIsNew](../P/PageIsNew.md)
  - BRIN_IS_REGULAR_PAGE
  - BrinPageType
  - [brin_start_evacuating_page](../b/brin_start_evacuating_page.md)
  - [brin_evacuate_page](../b/brin_evacuate_page.md)
  - [brin_page_init](../b/brin_page_init.md)
  - [MarkBufferDirty](../M/MarkBufferDirty.md)
  - RelationNeedsWAL
  - [XLogBeginInsert](../X/XLogBeginInsert.md)
  - [XLogRegisterData](../X/XLogRegisterData.md)
  - [XLogRegisterBuffer](../X/XLogRegisterBuffer.md)
  - [XLogInsert](../X/XLogInsert.md)
  - [PageSetLSN](../P/PageSetLSN.md)
- Called from (representative examples):
  - [revmap_extend_and_get_blkno](revmap_extend_and_get_blkno.md)

## Notes and Other Information
- This is a static function used internally within the BRIN revmap implementation
- The function handles several edge cases including concurrent relation extensions and page evacuation
- Uses critical sections (START_CRIT_SECTION/END_CRIT_SECTION) to ensure atomicity of metadata updates
- Implements proper WAL logging for crash recovery when RelationNeedsWAL returns true
- The function may return early without extending if concurrency conflicts are detected, requiring the caller to retry
- Page evacuation is performed when a target block is already in use as a regular BRIN page
- Maintains proper buffer locking protocols to prevent corruption during concurrent access
- Updates the metapage's pd_lower field to ensure proper page compression handling by xlog.c

## Simplified Source

```c
static void revmap_physical_extend(BrinRevmap *revmap)
{
    // Lock metapage to prevent concurrent extensions
    LockBuffer(revmap->rm_metaBuf, BUFFER_LOCK_EXCLUSIVE);
    Page metapage = BufferGetPage(revmap->rm_metaBuf);
    BrinMetaPageData *metadata = (BrinMetaPageData *) PageGetContents(metapage);

    // Check if our cached metadata is current; if not, update and retry
    if (metadata->lastRevmapPage != revmap->rm_lastRevmapPage) {
        revmap->rm_lastRevmapPage = metadata->lastRevmapPage;
        LockBuffer(revmap->rm_metaBuf, BUFFER_LOCK_UNLOCK);
        return;  // Caller should retry
    }

    BlockNumber mapBlk = metadata->lastRevmapPage + 1;

    // Get or create the target block
    Buffer buf;
    BlockNumber nblocks = RelationGetNumberOfBlocks(revmap->rm_irel);
    if (mapBlk < nblocks) {
        buf = ReadBuffer(revmap->rm_irel, mapBlk);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    } else {
        buf = ExtendBufferedRel(BMR_REL(revmap->rm_irel), MAIN_FORKNUM, NULL, EB_LOCK_FIRST);
        if (BufferGetBlockNumber(buf) != mapBlk) {
            // Concurrent extension detected, retry
            LockBuffer(revmap->rm_metaBuf, BUFFER_LOCK_UNLOCK);
            UnlockReleaseBuffer(buf);
            return;
        }
    }

    Page page = BufferGetPage(buf);

    // Validate page type
    if (!PageIsNew(page) && !BRIN_IS_REGULAR_PAGE(page))
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                       errmsg("unexpected page type 0x%04X in BRIN index \"%s\" block %u",
                              BrinPageType(page), RelationGetRelationName(revmap->rm_irel),
                              BufferGetBlockNumber(buf))));

    // Handle page evacuation if needed
    if (brin_start_evacuating_page(revmap->rm_irel, buf)) {
        LockBuffer(revmap->rm_metaBuf, BUFFER_LOCK_UNLOCK);
        brin_evacuate_page(revmap->rm_irel, revmap->rm_pagesPerRange, revmap, buf);
        return;  // Caller should retry
    }

    START_CRIT_SECTION();

    // Initialize as revmap page and update metadata
    brin_page_init(page, BRIN_PAGETYPE_REVMAP);
    MarkBufferDirty(buf);

    metadata->lastRevmapPage = mapBlk;

    // Fix metapage pd_lower for compression
    ((PageHeader) metapage)->pd_lower =
        ((char *) metadata + sizeof(BrinMetaPageData)) - (char *) metapage;
    MarkBufferDirty(revmap->rm_metaBuf);

    // WAL logging if needed
    if (RelationNeedsWAL(revmap->rm_irel)) {
        xl_brin_revmap_extend xlrec;
        xlrec.targetBlk = mapBlk;

        XLogBeginInsert();
        XLogRegisterData((char *) &xlrec, SizeOfBrinRevmapExtend);
        XLogRegisterBuffer(0, revmap->rm_metaBuf, REGBUF_STANDARD);
        XLogRegisterBuffer(1, buf, REGBUF_WILL_INIT);

        XLogRecPtr recptr = XLogInsert(RM_BRIN_ID, XLOG_BRIN_REVMAP_EXTEND);
        PageSetLSN(metapage, recptr);
        PageSetLSN(page, recptr);
    }

    END_CRIT_SECTION();

    LockBuffer(revmap->rm_metaBuf, BUFFER_LOCK_UNLOCK);
    UnlockReleaseBuffer(buf);
}
```