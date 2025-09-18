# writeListPage

## Location
src/backend/access/gin/ginfast.c: 59 - 144

## Overview
A static function that builds a pending-list page from an array of index tuples and writes it to a buffer, returning the amount of free space remaining on the page.

## Definition


## Detailed Description
This function is responsible for constructing GIN pending-list pages during fast insertion operations. It takes an array of index tuples and organizes them into a properly formatted page structure. The function operates within a critical section to ensure atomicity and handles WAL logging when necessary. It initializes the buffer as a GIN_LIST page type, adds each tuple to the page sequentially, sets up the page's opaque data including rightlink information, and marks the buffer as dirty. For tail pages (rightlink == InvalidBlockNumber), it sets special flags indicating the page contains complete rows.

## Parameters / Member Variables
- `index`: The GIN index relation being modified
- `buffer`: The buffer where the page will be written
- `tuples`: Array of IndexTuple pointers to be added to the page
- `ntuples`: Number of tuples in the array
- `rightlink`: Block number of the next page in the list, or InvalidBlockNumber for tail pages

## Dependencies
- Functions called/Symbols referenced:
  - [GinInitBuffer](../G/GinInitBuffer.md)
  - PageAddItem
  - GinPageGetOpaque
  - GinPageSetFullRow
  - [XLogBeginInsert](../X/XLogBeginInsert.md)
  - [XLogRegisterData](../X/XLogRegisterData.md)
  - [XLogRegisterBuffer](../X/XLogRegisterBuffer.md)
  - [XLogRegisterBufData](../X/XLogRegisterBufData.md)
  - [XLogInsert](../X/XLogInsert.md)
  - [PageGetExactFreeSpace](../P/PageGetExactFreeSpace.md)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)
- Called from (representative examples):
  - [makeSublist](../m/makeSublist.md)

## Notes and Other Information
- Operates within START_CRIT_SECTION/END_CRIT_SECTION for atomic operations
- Uses PGAlignedBlock workspace for temporary tuple storage during WAL logging
- Handles WAL logging when RelationNeedsWAL(index) is true
- Sets page flags differently for tail pages vs intermediate pages
- Returns exact free space remaining on the page for capacity planning
- Part of GIN's fast insertion mechanism for pending list management