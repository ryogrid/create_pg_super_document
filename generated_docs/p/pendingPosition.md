# pendingPosition

## Location
src/backend/access/gin/ginget.c: 29 - 36

## Overview
The  structure tracks position and state information while scanning through GIN (Generalized Inverted Index) pending list entries during index retrieval operations.

## Definition


## Detailed Description
The  structure is an internal data structure used by PostgreSQL's GIN access method to maintain state while scanning through the pending list during index searches. It encapsulates all necessary information to track the current position within a buffer page and manage the scanning process across multiple tuples within that page.

This structure is primarily used during GIN index scans when processing entries from the pending list - a temporary storage area for index entries that haven't yet been merged into the main GIN index structure. The pending list allows for faster insertions by deferring the more expensive merge operations.

## Parameters / Member Variables
- : Buffer containing the current page being scanned from the pending list
- : The first offset number within the current page range being processed
- : The last offset number within the current page range being processed  
- : ItemPointerData structure containing the current heap tuple identifier (TID)
- : Pointer to a boolean array indicating which scan keys have matches for the current item

## Dependencies
- Functions called/Symbols referenced:
  - Buffer (buffer management type)
  - OffsetNumber (page offset type)
  - ItemPointerData (heap tuple identifier type)

- Called from (representative examples):
  - scanGetCandidate (src/backend/access/gin/ginget.c:1454)
  - collectMatchesForHeapRow (src/backend/access/gin/ginget.c:1609)
  - scanPendingInsert (src/backend/access/gin/ginget.c:1831)

## Notes and Other Information
- This structure is used exclusively within the GIN access method implementation for managing pending list scans
- The buffer is expected to be pinned and share-locked during operations using this structure
- The hasMatchKey array is used to track which scan keys have found matches, enabling efficient multi-key index searches
- This is an internal structure not exposed to users of the GIN index interface