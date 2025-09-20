# log_newpage

## Location
[src/backend/access/transam/xloginsert.c:1143-1174](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xloginsert.c#L1143-L1174)

## Overview
log_newpage writes a WAL record containing a full image of a page for crash recovery, typically used when building pages in private memory.

## Definition

```c
XLogRecPtr
log_newpage(RelFileLocator *rlocator, ForkNumber forknum, BlockNumber blkno,
			Page page, bool page_std)
```
## Detailed Description
log_newpage creates a WAL record with a complete full-page image of the provided page data. This function is designed for scenarios where pages are constructed in private memory and then written directly to storage via the storage manager (smgr), bypassing the buffer manager. It forces the inclusion of the page image in the WAL record and optionally optimizes standard page layouts by excluding unused space between pd_lower and pd_upper. After writing the WAL record, it updates the page's LSN unless the page is uninitialized. The caller is responsible for actually writing the page to disk after calling this function.

## Parameters / Member Variables
- : Pointer to the relation file locator identifying the relation
- : Fork number (main, FSM, visibility map, etc.)
- : Block number within the relation fork
- : Pointer to the page data to be logged
- : Whether the page follows standard layout (enables space optimization)

## Dependencies
- Functions called/Symbols referenced:
  - [XLogBeginInsert](../X/XLogBeginInsert.md) (starts WAL record construction)
  - [XLogRegisterBlock](../X/XLogRegisterBlock.md) (registers the page with WAL record)
  - [XLogInsert](../X/XLogInsert.md) (finalizes and writes WAL record with XLOG_FPI type)
  - [PageIsNew](../P/PageIsNew.md) (checks if page is uninitialized)
  - [PageSetLSN](../P/PageSetLSN.md) (sets the page's log sequence number)
  - REGBUF_FORCE_IMAGE (forces full page image inclusion)
  - REGBUF_STANDARD (optimizes standard page layout)
- Called from:
  - [_hash_init](../h/_hash_init.md) (during hash index initialization)
  - [_hash_alloc_buckets](../h/_hash_alloc_buckets.md) (when allocating hash index buckets)
  - [log_newpage_buffer](log_newpage_buffer.md) (as part of buffer-based page logging)

## Notes and Other Information
- Intended for pages built in private memory, not buffer-based pages
- Caller must write the actual page to disk after calling this function
- Forces full page image inclusion regardless of other WAL settings
- Optimizes standard pages by excluding pd_lower/pd_upper unused space
- Avoids setting LSN on uninitialized pages to prevent corruption
- Uses XLOG_FPI WAL record type for full page images
- For buffer-based operations, use log_newpage_buffer instead