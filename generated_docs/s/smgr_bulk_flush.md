# smgr_bulk_flush

## Location
[src/backend/storage/smgr/bulk_write.c:243-323](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/bulk_write.c#L243-L323)

## Overview
A static function that writes all pending bulk write operations to disk, handling WAL logging, page checksums, and proper extension of relation files.

## Definition
```c
static void smgr_bulk_flush(BulkWriteState *bulkstate)
```

## Detailed Description
This function performs the actual disk I/O for bulk write operations by processing all pending writes in the BulkWriteState. It first sorts the pending writes by block number for optimal I/O performance, then optionally WAL-logs the pages using log_newpages. For each page, it sets checksums, handles file extension when writing beyond the current relation size, and fills gaps with zero pages to prevent file fragmentation. The function distinguishes between extending the relation (for new blocks) and overwriting existing blocks.

## Parameters / Member Variables
- `bulkstate`: The BulkWriteState containing pending writes and operation metadata

## Dependencies
- Functions called/Symbols referenced:
  - qsort
  - [buffer_cmp](../b/buffer_cmp.md)
  - [log_newpages](../l/log_newpages.md)
  - [PageSetChecksumInplace](../P/PageSetChecksumInplace.md)
  - [smgrextend](smgrextend.md)
  - [smgrwrite](smgrwrite.md)
  - [pfree](../p/pfree.md)
  - [PendingWrite](../P/PendingWrite.md)
  - MAX_PENDING_WRITES
  - zero_buffer
- Called from (representative examples):
  - [smgr_bulk_finish](smgr_bulk_finish.md)
  - [smgr_bulk_write](smgr_bulk_write.md) (when buffer is full)

## Notes and Other Information
- This is a static function internal to the bulk_write.c module
- Sorts pending writes by block number to optimize disk I/O patterns and reduce seeking
- Uses log_newpages for efficient WAL logging of multiple pages in batches
- Handles mixed standard and non-standard page layouts by logging all pages as non-standard if any are detected
- Fills gaps between current relation size and target block with zero pages to prevent file fragmentation
- Sets page checksums just before writing to ensure data integrity
- Resets the npending counter to 0 after flushing all writes
- The skipFsync parameter is set to true for all writes, deferring fsync responsibility to the finish function

## Simplified Source

```c
static void smgr_bulk_flush(BulkWriteState *bulkstate) {
    int npending = bulkstate->npending;
    PendingWrite *pending_writes = bulkstate->pending_writes;

    if (npending == 0)
        return;

    // Sort writes by block number for optimal I/O performance
    if (npending > 1)
        qsort(pending_writes, npending, sizeof(PendingWrite), buffer_cmp);

    // WAL log all pages if WAL is enabled
    if (bulkstate->use_wal) {
        BlockNumber blknos[MAX_PENDING_WRITES];
        Page pages[MAX_PENDING_WRITES];
        bool page_std = true;

        // Collect block numbers and pages for batch logging
        for (int i = 0; i < npending; i++) {
            blknos[i] = pending_writes[i].blkno;
            pages[i] = pending_writes[i].buf->data;
            if (!pending_writes[i].page_std)
                page_std = false;
        }

        log_newpages(&bulkstate->smgr->smgr_rlocator.locator,
                     bulkstate->forknum, npending, blknos, pages, page_std);
    }

    // Write each page to disk
    for (int i = 0; i < npending; i++) {
        BlockNumber blkno = pending_writes[i].blkno;
        Page page = pending_writes[i].buf->data;

        // Set page checksum before writing
        PageSetChecksumInplace(page, blkno);

        if (blkno >= bulkstate->relsize) {
            // Fill gaps with zero pages to prevent fragmentation
            while (blkno > bulkstate->relsize) {
                smgrextend(bulkstate->smgr, bulkstate->forknum,
                          bulkstate->relsize, &zero_buffer, true);
                bulkstate->relsize++;
            }
            // Extend relation with new page
            smgrextend(bulkstate->smgr, bulkstate->forknum, blkno, page, true);
            bulkstate->relsize++;
        } else {
            // Overwrite existing page
            smgrwrite(bulkstate->smgr, bulkstate->forknum, blkno, page, true);
        }

        pfree(page);
    }

    bulkstate->npending = 0;
}
```