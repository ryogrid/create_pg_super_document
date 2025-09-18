# addItemPointersToLeafTuple

## Location
src/backend/access/gin/gininsert.c: 46 - 125

## Overview
Adds an array of item pointers to a GIN index tuple's posting list, or creates a posting tree if the tuple would become too large.

## Definition


## Detailed Description
This function takes an existing GIN index tuple and adds new item pointers to its posting list. The function handles two scenarios:

1. **Small posting list**: If the resulting tuple can fit within the maximum tuple size (GinMaxItemSize), it creates a new tuple with a compressed posting list containing both old and new item pointers.

2. **Large posting list**: If the combined posting list would make the tuple too large, it converts to a posting tree structure. It creates a posting tree from the old tuple's posting list, inserts the new items into the tree, and returns a tuple that points to this posting tree.

The function ensures that all item pointers are properly merged and sorted, with no duplicates. It uses compression to minimize storage space and automatically handles the transition from posting lists to posting trees when necessary.

## Parameters / Member Variables
- : GIN access method state information containing index configuration
- : The existing index tuple to be modified (must not already be a posting tree)
- : Array of new item pointers to add (must be sorted with no duplicates)
- : Number of item pointers in the items array
- : Statistics collection structure for tracking build progress
- : Buffer containing the page where operations are performed

## Dependencies
- Functions called/Symbols referenced:
  - [gintuple_get_attrnum](../g/gintuple_get_attrnum.md): Extract attribute number from tuple
  - [gintuple_get_key](../g/gintuple_get_key.md): Extract key value from tuple
  - [ginReadTuple](../g/ginReadTuple.md): Read posting list from existing tuple
  - [ginMergeItemPointers](../g/ginMergeItemPointers.md): Merge old and new item pointer arrays
  - [ginCompressPostingList](../g/ginCompressPostingList.md): Compress posting list for storage
  - [GinFormTuple](../G/GinFormTuple.md): Create new GIN index tuple
  - [createPostingTree](../c/createPostingTree.md): Create posting tree when tuple becomes too large
  - [ginInsertItemPointers](../g/ginInsertItemPointers.md): Insert items into posting tree
  - GinSetPostingTree: Set posting tree reference in tuple

- Called from (representative examples):
  - [ginEntryInsert](../g/ginEntryInsert.md): Main entry point for inserting items into GIN index

## Notes and Other Information
- The function assumes the input items array is already sorted and contains no duplicates
- Uses GinMaxItemSize constant to determine when to convert to posting tree
- Memory management: Properly frees allocated memory for compressed lists and merged arrays
- The function is static, indicating it's an internal implementation detail of GIN insertion
- Critical for GIN index performance as it handles the transition between different storage formats
- Part of the GIN access method's strategy for handling variable-sized posting lists efficiently