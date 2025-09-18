# buildFreshLeafTuple

## Location
src/backend/access/gin/gininsert.c: 126 - 175

## Overview
Builds a fresh GIN index leaf tuple from scratch, choosing between posting-list or posting-tree format based on the size of the item pointers array.

## Definition


## Detailed Description
This function creates a new GIN index tuple from the provided key and item pointers. It implements the same core logic as addItemPointersToLeafTuple but works from different input parameters (individual components rather than an existing tuple).

The function follows a two-stage approach:

1. **Attempt posting list format**: First tries to compress the item pointers into a posting list and create a tuple that fits within GinMaxItemSize limits.

2. **Fall back to posting tree**: If the compressed posting list would make the tuple too large, it creates a posting tree to store the item pointers and builds a tuple that references this tree.

This dual approach ensures efficient storage for small posting lists while seamlessly scaling to handle large collections of item pointers through posting trees.

## Parameters / Member Variables
- : GIN access method state information containing index configuration
- : Attribute number for the key being indexed
- : The actual key value being indexed
- : Category information for null handling (GIN_CAT_NORM_KEY, etc.)
- : Array of item pointers to include (must be sorted with no duplicates)
- : Number of item pointers in the items array
- : Statistics collection structure for tracking build progress
- : Buffer for page operations during posting tree creation

## Dependencies
- Functions called/Symbols referenced:
  - [ginCompressPostingList](../g/ginCompressPostingList.md): Compress item pointers into posting list format
  - [GinFormTuple](../G/GinFormTuple.md): Create GIN index tuple with posting list or tree reference
  - [createPostingTree](../c/createPostingTree.md): Create posting tree when list becomes too large
  - GinSetPostingTree: Set posting tree block number in tuple
  - SizeOfGinPostingList: Calculate size of compressed posting list

- Called from (representative examples):
  - [ginEntryInsert](../g/ginEntryInsert.md): Main entry point for GIN index insertion operations

## Notes and Other Information
- Similar to addItemPointersToLeafTuple but works from component values rather than modifying existing tuple
- Input items array must be pre-sorted and contain no duplicates
- Uses same size threshold (GinMaxItemSize) to determine storage format
- Creates posting tree tuple first when needed to fail quickly if key is too large
- Memory management: Properly frees compressed posting list after tuple creation
- Part of GIN's adaptive storage strategy for handling variable-sized posting collections
- Function is static, indicating internal implementation detail of GIN insertion logic