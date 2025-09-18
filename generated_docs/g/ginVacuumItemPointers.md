# ginVacuumItemPointers

## Location
src/backend/access/gin/ginvacuum.c: 48 - 89

## Overview
Vacuums an uncompressed posting list by removing dead tuple identifiers and returns a new array containing only the remaining valid items.

## Definition


## Detailed Description
This function processes an array of ItemPointer entries (TIDs) during GIN index vacuum operations. It iterates through each item in the posting list and uses a callback function to determine which items should be removed as dead tuples. The function optimizes memory allocation by only creating a new array when deletions are actually needed. If no items need to be removed, it returns NULL; otherwise, it returns a palloc'd array containing only the surviving items.

The function maintains vacuum statistics by incrementing counters for removed tuples and remaining index tuples in the GinVacuumState structure.

## Parameters / Member Variables
- : GinVacuumState pointer containing vacuum context, callback function, and result statistics
- : Array of ItemPointerData structures representing the posting list to be vacuumed
- : Number of items in the input array
- : Output parameter that receives the count of remaining items after vacuum

## Dependencies
- Functions called/Symbols referenced:
  - GinVacuumState (struct type)
  - callback (function pointer in gvs)
  - palloc (memory allocation)
  - memcpy (memory copy)
- Called from (representative examples):
  - ginVacuumPostingTreeLeaf
  - ginVacuumEntryPage

## Notes and Other Information
- Returns NULL if no items need to be removed, avoiding unnecessary memory allocation
- Memory allocation is deferred until the first item to be deleted is encountered
- Updates vacuum statistics (tuples_removed and num_index_tuples) in the GinVacuumState
- The returned array (if not NULL) must be freed by the caller
- Used specifically for uncompressed posting lists in GIN indexes