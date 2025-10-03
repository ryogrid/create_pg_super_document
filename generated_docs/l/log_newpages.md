# log_newpages

## Location
[src/backend/access/transam/xloginsert.c:1175-1236](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xloginsert.c#L1175-L1236)

## Overview
log_newpages efficiently writes WAL records for multiple page images in batches, optimizing performance compared to individual page logging.

## Definition

```c
void
log_newpages(RelFileLocator *rlocator, ForkNumber forknum, int num_pages,
			 BlockNumber *blknos, Page *pages, bool page_std)
```
## Detailed Description
log_newpages provides an efficient way to log multiple full-page images to WAL in a single operation. It processes pages in batches limited by XLR_MAX_BLOCK_ID, creating one WAL record per batch to minimize WAL record overhead. This is significantly more efficient than calling log_newpage() for each page individually when dealing with multiple pages. The function forces full-page images for all pages and supports standard page layout optimization. After writing each batch, it updates the LSN for all non-uninitialized pages in that batch. The caller remains responsible for writing the actual pages to disk.

## Parameters / Member Variables  
- `*rlocator`: Pointer to the relation file locator identifying the relation
- `forknum`: Fork number (main, FSM, visibility map, etc.)
- `num_pages`: Total number of pages to be logged
- `*blknos`: Array of block numbers corresponding to each page
- `*pages`: Array of pointers to page data to be logged
- `page_std`: Whether all pages follow standard layout (enables space optimization)
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

## Simplified Source

```c
void
log_newpages(RelFileLocator *rlocator, ForkNumber forknum, int num_pages,
             BlockNumber *blknos, Page *pages, bool page_std)
{
    int flags = REGBUF_FORCE_IMAGE;
    if (page_std)
        flags |= REGBUF_STANDARD;

    // Ensure space for maximum batch size
    XLogEnsureRecordSpace(XLR_MAX_BLOCK_ID - 1, 0);

    // Process pages in batches of up to XLR_MAX_BLOCK_ID
    int i = 0;
    while (i < num_pages) {
        int batch_start = i;
        int nbatch;

        XLogBeginInsert();

        // Fill current batch
        nbatch = 0;
        while (nbatch < XLR_MAX_BLOCK_ID && i < num_pages) {
            XLogRegisterBlock(nbatch, rlocator, forknum, blknos[i], pages[i], flags);
            i++;
            nbatch++;
        }

        // Write WAL record for this batch
        XLogRecPtr recptr = XLogInsert(RM_XLOG_ID, XLOG_FPI);

        // Set LSN on all non-uninitialized pages in this batch
        for (int j = batch_start; j < i; j++) {
            if (!PageIsNew(pages[j])) {
                PageSetLSN(pages[j], recptr);
            }
        }
    }
}
```