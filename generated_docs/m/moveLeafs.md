# moveLeafs

## Location
src/backend/access/spgist/spgdoinsert.c: 387 - 567

## Overview
This function moves an entire chain of leaf tuples from one page to another when there isn't enough room to add a new leaf tuple to the current page, used as an alternative to splitting when the chain contains little data.

## Definition


## Detailed Description
This function implements a space optimization strategy for SPGiST indexes. When a leaf tuple chain needs more space but contains very little data (making a split inefficient), it moves the entire chain to a new page along with the new tuple that couldn't fit. The function:

1. Analyzes the current chain to determine space requirements
2. Finds or allocates a new leaf page with sufficient space
3. Copies all live tuples from the old chain to the new page (reversing chain order)
4. Adds the new tuple to the chain on the new page
5. Deletes old tuples and leaves redirection pointers (unless during index build)
6. Updates the parent's downlink to point to the new location
7. Handles WAL logging for crash recovery

The function cannot work on root pages and includes special handling for DEAD tuples.

## Parameters / Member Variables
- : The SPGiST index relation being modified
- : SPGiST state information containing configuration and temporary data
- : Page descriptor for the current page containing the tuple chain to move
- : Page descriptor for the parent page (must be valid, not root)
- : The new leaf tuple that triggered the move operation
- : Boolean indicating if this is a nulls page

## Dependencies
- Functions called/Symbols referenced:
  - SpGistGetBuffer (allocates a new leaf page with required space)
  - SpGistPageAddNewItem (adds items to the new page)
  - spgPageIndexMultiDelete (deletes old tuples and sets redirection)
  - saveNodeLink (updates parent's downlink)
  - SGLT_SET_NEXTOFFSET/SGLT_GET_NEXTOFFSET (chain manipulation macros)
  - Various page access functions (PageGetItem, PageGetItemId, etc.)
  - WAL logging functions (XLogBeginInsert, XLogInsert, etc.)
- Called from (representative examples):
  - spgdoinsert (at src/backend/access/spgist/spgdoinsert.c:2133)

## Notes and Other Information
- Cannot operate on root pages (assertion enforced)
- Reverses the order of tuples in the chain during the move (but this doesn't affect correctness)
- DEAD tuples are deleted but not moved to the new page
- Uses SPGIST_REDIRECT pointers for the first deleted tuple (unless during index build)
- Subsequent deleted tuples get SPGIST_PLACEHOLDER markers
- Operates within critical section for atomicity
- Updates free-space cache with SpGistSetLastUsedPage()
- Supports WAL logging when RelationNeedsWAL() returns true
- Uses spgxlogMoveLeafs structure for WAL record information
- Location: src/backend/access/spgist/spgdoinsert.c:387-567