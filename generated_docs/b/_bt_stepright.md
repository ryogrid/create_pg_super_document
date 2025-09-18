# _bt_stepright

## Location
src/backend/access/nbtree/nbtinsert.c: 1027 - 1104

## Overview
Steps right to the next non-dead leaf page during insertion while maintaining proper write-lock ordering to prevent concurrency issues.

## Definition


## Detailed Description
The  function moves the insertion context from the current leaf page to the next suitable leaf page to the right. This operation is more complex than a simple search movement because it must maintain strict locking protocols to ensure that concurrent uniqueness checking operations can see insertions correctly.

The function implements a crucial locking protocol: it acquires a write lock on the target page before releasing the write lock on the current page. This prevents other transactions' uniqueness scans from missing the insertion that's in progress. Without this careful ordering, a concurrent transaction could incorrectly conclude that a duplicate doesn't exist.

The function also handles special cases like incomplete page splits and dead/ignored pages, ensuring that the insertion proceeds to a valid, usable leaf page.

## Parameters / Member Variables
- : The B-tree index relation being operated on
- : The heap relation associated with the index (must not be NULL)
- : Current insertion state to be updated with new buffer
- : Search stack needed for potential split completion

## Dependencies
- Functions called/Symbols referenced:
  - _bt_relandgetbuf: Releases current buffer and acquires new one with specified lock
  - _bt_finish_split: Completes any incomplete page splits encountered
  - _bt_relbuf: Releases buffer lock and pin
  - P_INCOMPLETE_SPLIT: Checks if page has incomplete split
  - P_IGNORE: Checks if page should be ignored (dead)
  - P_RIGHTMOST: Checks if page is rightmost in tree
- Called from (representative examples):
  - _bt_findinsertloc: When searching for optimal insertion location

## Notes and Other Information
- Updates insertstate->buf to point to the new buffer and invalidates cached bounds
- Maintains write lock on target page while releasing lock on source page for concurrency safety
- Handles incomplete page splits by completing them before proceeding
- Skips over ignored/dead pages to find next valid insertion target
- More aggressive locking than strictly necessary for non-unique indexes, but ensures correctness
- Will error if it encounters the rightmost page while looking for ignored pages
- Critical for maintaining consistency in concurrent unique index operations