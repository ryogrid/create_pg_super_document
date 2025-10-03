# brin_xlog_revmap_extend

## Location
[src/backend/access/brin/brin_xlog.c:208-268](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_xlog.c#L208-L268)

## Overview
Replays a BRIN revmap page extension operation during WAL recovery, updating the metapage and initializing the new revmap page.

## Definition

```c
static void
brin_xlog_revmap_extend(XLogReaderState *record)
```
## Detailed Description
This function handles the replay of a BRIN revmap page extension during crash recovery. When a BRIN index needs to extend its revmap (reverse mapping from heap block numbers to summary pages), this operation is logged in WAL. During recovery, this function:

1. Extracts the xl_brin_revmap_extend record from the WAL entry
2. Updates the metapage to reflect the new lastRevmapPage value
3. Initializes the target block as a new revmap page
4. Sets appropriate LSNs and marks buffers dirty

The function ensures that the BRIN index's revmap structure is correctly reconstructed during recovery, maintaining the mapping between heap blocks and their corresponding summary pages.

## Parameters / Member Variables
- `*record`: XLogReaderState pointer containing the WAL record being replayed, including the revmap extension data and affected block information
## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData: Extract record data from WAL
  - [XLogRecGetBlockTag](../X/XLogRecGetBlockTag.md): Get block information from WAL record
  - [XLogReadBufferForRedo](../X/XLogReadBufferForRedo.md): Read and prepare buffer for redo operation
  - [XLogInitBufferForRedo](../X/XLogInitBufferForRedo.md): Initialize buffer for redo operation
  - [BufferGetPage](../B/BufferGetPage.md): Get page from buffer
  - [PageGetContents](../P/PageGetContents.md): Get page contents
  - [brin_page_init](brin_page_init.md): Initialize BRIN page with specific type
  - [PageSetLSN](../P/PageSetLSN.md): Set LSN on page
  - [MarkBufferDirty](../M/MarkBufferDirty.md): Mark buffer as modified
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md): Release buffer locks
- Called from (representative examples):
  - [brin_redo](brin_redo.md): Main BRIN WAL replay dispatcher function

## Notes and Other Information
- This is a static function used only within the BRIN WAL replay subsystem
- The function handles both metapage updates and new revmap page initialization atomically
- Includes assertion checks to validate that the target block matches the expected value
- Properly handles pd_lower setting on metapage to prevent data loss during page compression
- Part of PostgreSQL's crash recovery mechanism for BRIN indexes

## Simplified Source

```c
static void brin_xlog_revmap_extend(XLogReaderState *record)
{
    XLogRecPtr lsn = record->EndRecPtr;
    xl_brin_revmap_extend *extend_data = (xl_brin_revmap_extend *) XLogRecGetData(record);
    Buffer metabuf, revmap_buf;

    // Step 1: Update the metapage with new lastRevmapPage
    XLogRedoAction action = XLogReadBufferForRedo(record, 0, &metabuf);
    if (action == BLK_NEEDS_REDO) {
        Page metapage = BufferGetPage(metabuf);
        BrinMetaPageData *metadata = (BrinMetaPageData *) PageGetContents(metapage);

        // Update the last revmap page number
        metadata->lastRevmapPage = extend_data->targetBlk;

        // Set proper page boundaries to prevent compression issues
        ((PageHeader) metapage)->pd_lower =
            ((char *) metadata + sizeof(BrinMetaPageData)) - (char *) metapage;

        PageSetLSN(metapage, lsn);
        MarkBufferDirty(metabuf);
    }

    // Step 2: Initialize the new revmap page
    revmap_buf = XLogInitBufferForRedo(record, 1);
    Page revmap_page = (Page) BufferGetPage(revmap_buf);

    brin_page_init(revmap_page, BRIN_PAGETYPE_REVMAP);
    PageSetLSN(revmap_page, lsn);
    MarkBufferDirty(revmap_buf);

    // Clean up
    UnlockReleaseBuffer(revmap_buf);
    if (BufferIsValid(metabuf))
        UnlockReleaseBuffer(metabuf);
}
```