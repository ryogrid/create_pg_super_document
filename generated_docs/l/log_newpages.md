# log_newpages

## Location
src/backend/access/transam/xloginsert.c: 1175 - 1236

## Overview
log_newpages efficiently writes WAL records for multiple page images in batches, optimizing performance compared to individual page logging.

## Definition


## Detailed Description
log_newpages provides an efficient way to log multiple full-page images to WAL in a single operation. It processes pages in batches limited by XLR_MAX_BLOCK_ID, creating one WAL record per batch to minimize WAL record overhead. This is significantly more efficient than calling log_newpage() for each page individually when dealing with multiple pages. The function forces full-page images for all pages and supports standard page layout optimization. After writing each batch, it updates the LSN for all non-uninitialized pages in that batch. The caller remains responsible for writing the actual pages to disk.

## Parameters / Member Variables  
- : Pointer to the relation file locator identifying the relation
- : Fork number (main, FSM, visibility map, etc.)
- : Total number of pages to be logged
- : Array of block numbers corresponding to each page
- : Array of pointers to page data to be logged
- : Whether all pages follow standard layout (enables space optimization)

## Dependencies
- Functions called/Symbols referenced:
  - [XLogEnsureRecordSpace](../X/XLogEnsureRecordSpace.md) (ensures sufficient space for maximum batch size)
  - [XLogBeginInsert](../X/XLogBeginInsert.md) (starts WAL record construction for each batch)
  - [XLogRegisterBlock](../X/XLogRegisterBlock.md) (registers each page in the current batch)
  - [XLogInsert](../X/XLogInsert.md) (finalizes and writes WAL record with XLOG_FPI type)
  - [PageIsNew](../P/PageIsNew.md) (checks if page is uninitialized before setting LSN)
  - [PageSetLSN](../P/PageSetLSN.md) (sets LSN on initialized pages)
  - XLR_MAX_BLOCK_ID (maximum blocks per WAL record)
  - REGBUF_FORCE_IMAGE (forces full page image inclusion)
  - REGBUF_STANDARD (optimizes standard page layout)
- Called from:
  - [smgr_bulk_flush](../s/smgr_bulk_flush.md) (during bulk write operations)

## Notes and Other Information
- More efficient than multiple log_newpage() calls for bulk operations
- Processes pages in batches of up to XLR_MAX_BLOCK_ID pages per WAL record
- Forces full page images regardless of other WAL settings
- All pages must be from the same relation and fork
- Caller must write actual pages to disk after calling this function
- Avoids setting LSN on uninitialized pages to prevent corruption
- Uses XLOG_FPI WAL record type for each batch of full page images
- Optimizes WAL space when page_std is true by excluding unused page areas