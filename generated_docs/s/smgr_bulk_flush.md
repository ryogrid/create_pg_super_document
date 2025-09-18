# smgr_bulk_flush

## Location
src/backend/storage/smgr/bulk_write.c: 243 - 323

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
  - buffer_cmp
  - log_newpages
  - PageSetChecksumInplace
  - smgrextend
  - smgrwrite
  - pfree
  - PendingWrite
  - MAX_PENDING_WRITES
  - zero_buffer
- Called from (representative examples):
  - smgr_bulk_finish
  - smgr_bulk_write (when buffer is full)

## Notes and Other Information
- This is a static function internal to the bulk_write.c module
- Sorts pending writes by block number to optimize disk I/O patterns and reduce seeking
- Uses log_newpages for efficient WAL logging of multiple pages in batches
- Handles mixed standard and non-standard page layouts by logging all pages as non-standard if any are detected
- Fills gaps between current relation size and target block with zero pages to prevent file fragmentation
- Sets page checksums just before writing to ensure data integrity
- Resets the npending counter to 0 after flushing all writes
- The skipFsync parameter is set to true for all writes, deferring fsync responsibility to the finish function