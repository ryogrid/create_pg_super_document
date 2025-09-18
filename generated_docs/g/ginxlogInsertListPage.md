# ginxlogInsertListPage

## Location
src/include/access/ginxlog.h: 182 - 187

## Overview
A WAL (Write-Ahead Logging) record structure used to log the insertion of tuples into a GIN index pending list page for crash recovery purposes.

## Definition


## Detailed Description
The  structure is used in PostgreSQL's GIN (Generalized Inverted Index) access method to record WAL entries when inserting tuples into pending list pages. This structure is part of the crash recovery mechanism that ensures data consistency by logging changes before they are applied to disk pages.

When new tuples are inserted into a GIN index's pending list (fast insertion mechanism), this structure captures the essential information needed to replay the operation during recovery. The pending list is a temporary storage area for newly inserted entries that haven't yet been moved into the main GIN tree structure.

The structure is followed by the actual tuple data in the WAL record, allowing complete reconstruction of the insertion operation during recovery.

## Parameters / Member Variables
- : Block number of the next page in the pending list chain, or InvalidBlockNumber if this is the tail page
- : Number of tuples being inserted into the page
- Note: The actual tuple data follows this structure in the WAL record but is not part of the struct definition

## Dependencies
- Functions called/Symbols referenced:
  - None (this is a data structure)
- Called from (representative examples):
  - [writeListPage](../w/writeListPage.md) (src/backend/access/gin/ginfast.c:118, 125)
  - [ginRedoInsertListPage](ginRedoInsertListPage.md) (src/backend/access/gin/ginxlog.c:623)

## Notes and Other Information
- This structure is specifically designed for WAL logging and is not used for in-memory operations
- The structure is part of the GIN index's fast insertion mechanism, which allows for efficient bulk insertions
- The rightlink field maintains the linked list structure of pending pages, essential for proper list traversal during recovery
- The tuple data that follows this structure in the WAL record contains the actual index tuples being inserted
- Used with WAL record type XLOG_GIN_INSERT_LISTPAGE for crash recovery
- Located in src/include/access/ginxlog.h:182-187