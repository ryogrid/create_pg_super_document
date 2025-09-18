# log_newpage

## Location
src/backend/access/transam/xloginsert.c: 1143 - 1174

## Overview
log_newpage writes a WAL record containing a full image of a page for crash recovery, typically used when building pages in private memory.

## Definition


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
  - XLogBeginInsert (starts WAL record construction)
  - XLogRegisterBlock (registers the page with WAL record)
  - XLogInsert (finalizes and writes WAL record with XLOG_FPI type)
  - PageIsNew (checks if page is uninitialized)
  - PageSetLSN (sets the page's log sequence number)
  - REGBUF_FORCE_IMAGE (forces full page image inclusion)
  - REGBUF_STANDARD (optimizes standard page layout)
- Called from:
  - _hash_init (during hash index initialization)
  - _hash_alloc_buckets (when allocating hash index buckets)
  - log_newpage_buffer (as part of buffer-based page logging)

## Notes and Other Information
- Intended for pages built in private memory, not buffer-based pages
- Caller must write the actual page to disk after calling this function
- Forces full page image inclusion regardless of other WAL settings
- Optimizes standard pages by excluding pd_lower/pd_upper unused space
- Avoids setting LSN on uninitialized pages to prevent corruption
- Uses XLOG_FPI WAL record type for full page images
- For buffer-based operations, use log_newpage_buffer instead