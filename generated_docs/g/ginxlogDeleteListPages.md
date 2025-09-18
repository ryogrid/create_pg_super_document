# ginxlogDeleteListPages

## Location
[src/include/access/ginxlog.h:203-207](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/ginxlog.h#L203-L207)

## Overview
A WAL (Write-Ahead Logging) record structure used to log the deletion of multiple pages from a GIN index pending list during cleanup operations for crash recovery purposes.

## Definition


## Detailed Description
The  structure is used in PostgreSQL's GIN (Generalized Inverted Index) access method to record WAL entries when deleting multiple pages from the pending list during cleanup operations. This structure is part of the crash recovery mechanism that ensures data consistency when the pending list is being compacted or cleared.

When the GIN index's pending list grows too large, a cleanup operation (shift list) is performed to move entries from the pending list into the main index structure and delete the now-empty pending pages. This structure captures both the updated metadata and the count of deleted pages, allowing complete reconstruction of the cleanup operation during recovery.

The structure includes a complete copy of the GinMetaPageData to ensure the metadata state can be properly restored, including updated head/tail pointers and statistics about the pending list.

## Parameters / Member Variables
- : Complete copy of the GIN index metadata page data (GinMetaPageData) after the deletion operation, including updated head/tail pointers, pending page counts, and statistics
- : Number of pages that were deleted from the pending list during this operation

## Dependencies
- Functions called/Symbols referenced:
  - [GinMetaPageData](../G/GinMetaPageData.md) (src/include/access/ginblock.h:55)
- Called from (representative examples):
  - [shiftList](../s/shiftList.md) (src/backend/access/gin/ginfast.c:570, 650)
  - [ginRedoDeleteListPages](ginRedoDeleteListPages.md) (src/backend/access/gin/ginxlog.c:678)
  - [gin_desc](gin_desc.md) (src/backend/access/rmgrdesc/gindesc.c:174)

## Notes and Other Information
- This structure is specifically designed for WAL logging and is not used for in-memory operations
- Used during GIN index maintenance when the pending list is being cleaned up to move entries to the main index structure
- The complete metadata copy ensures that all pending list statistics (nPendingPages, nPendingHeapTuples, head, tail, etc.) are properly maintained during recovery
- Supports batch deletion of up to GIN_NDELETE_AT_ONCE pages in a single WAL record for efficiency
- Used with WAL record type XLOG_GIN_DELETE_LISTPAGE for crash recovery
- Critical for maintaining the integrity of the pending list linked structure during cleanup operations
- Located in src/include/access/ginxlog.h:203-207