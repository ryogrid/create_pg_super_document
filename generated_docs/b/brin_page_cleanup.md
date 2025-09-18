# brin_page_cleanup

## Location
src/backend/access/brin/brin_pageops.c: 624 - 689

## Overview
Initializes uninitialized BRIN index pages if necessary and records their current free space in the Free Space Map (FSM) for future allocation decisions.

## Definition
```c
void brin_page_cleanup(Relation idxrel, Buffer buf)
```

## Detailed Description
This function performs maintenance operations on BRIN index pages, primarily designed for use during vacuum operations. Its main purposes are to handle uninitialized pages that may result from relation extension followed by crashes, and to update the FSM with current free space information.

The function operates in several phases:
1. **New Page Detection**: Checks if the page is uninitialized (PageIsNew)
2. **Concurrency Control**: Uses relation extension locking to coordinate with concurrent relation extension operations 
3. **Page Initialization**: If the page is still new after proper locking, initializes it as an empty BRIN page
4. **Page Type Filtering**: Skips processing for meta pages and revmap pages as they don't store regular tuples
5. **FSM Update**: Records the current free space of regular index pages in the Free Space Map

The extension lock mechanism prevents race conditions where this function might initialize a page that another process is already extending and initializing. The double-check pattern (checking PageIsNew before and after acquiring locks) ensures proper coordination.

## Parameters / Member Variables
- `idxrel`: Relation structure representing the BRIN index being maintained
- `buf`: Buffer containing the page to clean up and potentially initialize

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](../B/BufferGetPage.md) (to access the page from buffer)
  - [PageIsNew](../P/PageIsNew.md) (to check if page is uninitialized)
  - LockRelationForExtension/UnlockRelationForExtension (for extension lock coordination)
  - [LockBuffer](../L/LockBuffer.md) (for buffer locking during initialization)
  - [brin_initialize_empty_new_buffer](brin_initialize_empty_new_buffer.md) (to initialize new pages)
  - BRIN_IS_META_PAGE (to check for meta pages)
  - BRIN_IS_REVMAP_PAGE (to check for revmap pages)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md) (to get the page's block number)
  - RecordPageWithFreeSpace (to update FSM)
  - [br_page_get_freespace](br_page_get_freespace.md) (to calculate available free space)
  - ShareLock, BUFFER_LOCK_EXCLUSIVE, BUFFER_LOCK_UNLOCK (lock constants)
- Called from:
  - [brin_vacuum_scan](brin_vacuum_scan.md) (in brin.c during vacuum operations)

## Notes and Other Information
- Primarily used during VACUUM operations to handle pages that may have been left uninitialized due to crashes
- Does not update upper FSM pages, expecting the caller (brin_vacuum_scan) to handle FSM updates at the end of the scan
- Uses a careful locking protocol to avoid race conditions with concurrent relation extension
- Only processes regular index pages, skipping meta pages and reverse mapping pages
- The function handles the case where a page might be initialized by another process between the initial check and acquiring the exclusive lock
- Part of the BRIN index maintenance infrastructure that ensures all pages are properly initialized and their free space is tracked
- The extension lock coordination is crucial for maintaining consistency in multi-process environments where relation extension might be happening concurrently