# ginRedoDeletePage

## Location
src/backend/access/gin/ginxlog.c: 477 - 527

## Overview
Replays GIN data page deletion operations during WAL recovery, updating sibling links and marking pages as deleted.

## Definition


## Detailed Description
ginRedoDeletePage is a WAL recovery function that replays GIN (Generalized Inverted Index) data page deletion operations from transaction log records. This function handles the complex process of removing a data page from the GIN index structure while maintaining consistency across multiple related pages.

The deletion process involves three pages:
1. **Deleted page** (dbuffer): The page being removed, marked as deleted with the transaction ID
2. **Left sibling page** (lbuffer): Updated to skip over the deleted page by updating its rightlink pointer  
3. **Parent page** (pbuffer): Updated to remove the posting item that pointed to the deleted page

The function carefully manages locking order to prevent deadlocks, specifically locking the left page first to avoid conflicts with the ginStepRight() function that traverses pages from left to right.

Key functionality:
- Updates left sibling's rightlink to skip the deleted page
- Marks the target page as deleted with the appropriate transaction ID
- Removes the posting item from the parent page that referenced the deleted page
- Maintains proper locking order to prevent deadlocks

## Parameters / Member Variables
- : XLogReaderState pointer containing the WAL record being replayed, including deletion metadata and affected page references

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - XLogReadBufferForRedo
  - BufferGetPage
  - GinPageIsData
  - GinPageGetOpaque
  - GinPageSetDeleted
  - GinPageSetDeleteXid
  - GinPageIsLeaf
  - GinPageDeletePostingItem
  - PageSetLSN
  - MarkBufferDirty
  - BufferIsValid
  - UnlockReleaseBuffer

- Data structures used:
  - ginxlogDeletePage

- Constants used:
  - BLK_NEEDS_REDO

- Called from:
  - gin_redo

## Notes and Other Information
- Handles three related pages in a specific order: left sibling first, then deleted page, then parent page
- Uses careful locking order (left page first) to prevent deadlocks with ginStepRight() function
- All affected pages must be data pages (verified with assertions)
- The parent page must not be a leaf page (only internal pages have posting items to delete)
- Transaction ID is recorded on the deleted page for proper visibility handling
- Part of PostgreSQL's GIN index deletion recovery mechanism ensuring structural consistency
- The ginxlogDeletePage structure contains rightLink, deleteXid, and parentOffset fields needed for recovery