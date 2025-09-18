# _bt_delitems_delete

## Location
[src/backend/access/nbtree/nbtpage.c:1284-1404](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtpage.c#L1284-L1404)

## Overview
Deletes and updates items on a btree leaf page during single-page cleanup operations, handling both complete item deletions and partial updates to posting list tuples by removing specific TIDs.

## Definition


## Detailed Description
This function performs deletion and update operations on a B-tree leaf page during single-page cleanup. It handles two types of operations:

1. **Complete item deletion**: Removes entire index tuples from the page
2. **Partial posting list updates**: Updates existing posting list items by removing specific heap TIDs while preserving others

The function is nearly identical to  in terms of page modifications, but differs in that it:
- Uses its own  and  parameters for recovery conflict generation
- Does NOT clear the page's VACUUM cycle ID (only  controls vacuum cycle IDs)

The function ensures WAL logging consistency and handles both deletions and updates atomically within a critical section.

## Parameters / Member Variables
- : The btree index relation being modified
- : Buffer containing the leaf page to modify (must be pinned and write-locked by caller)
- : Transaction ID for generating recovery conflicts during WAL replay
- : Boolean indicating if this is a catalog relation (affects conflict handling)
- : Array of offset numbers for items to be completely deleted (must be sorted ascending)
- : Number of items in the deletable array
- : Array of BTVacuumPosting structures for items to be partially updated
- : Number of items in the updatable array

## Dependencies
- Functions called/Symbols referenced:
  - : Generates new versions of posting lists without deleted TIDs
  - : Overwrites existing tuples with updated versions
  - : Deletes multiple items from the page
  - , , , , : WAL logging functions
  - , , : Page and relation utility functions
- Called from:
  - : Main entry point for single-page cleanup operations

## Notes and Other Information
- The caller must ensure the buffer is pinned and write-locked before calling this function
- Both deletable and updatable arrays must be sorted in ascending order by offset number
- The function operates within a critical section to ensure atomicity of changes
- Unlike vacuum operations, this function preserves the page's vacuum cycle ID
- WAL logging is conditional based on 
- The function clears the  flag to indicate removal of dead items
- Memory allocated for updated tuples is properly freed to prevent leaks