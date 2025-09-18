# ginInsertValue

## Location
src/backend/access/gin/ginbtree.c: 816 - 835

## Overview
ginInsertValue provides the high-level interface for inserting values into GIN B-trees, coordinating the insertion process and handling any necessary page splits or incomplete split completions.

## Definition
```c
void ginInsertValue(GinBtree btree, GinBtreeStack *stack, void *insertdata,
                   GinStatsData *buildStats)
```

## Detailed Description
ginInsertValue serves as the main entry point for GIN B-tree insertions, orchestrating the complete insertion workflow. The function follows a two-phase approach:

1. **Preparation Phase**: Checks for and resolves any incomplete splits on the target leaf page using ginFinishOldSplit
2. **Insertion Phase**: Attempts to place the value using ginPlaceToPage, then handles the outcome based on whether splits are required

The function abstracts the complexity of the insertion process from callers by automatically handling:
- **Incomplete Split Resolution**: Ensures the target page is in a consistent state before insertion
- **Split Management**: Automatically triggers split completion via ginFinishSplit when pages don't have sufficient space
- **Resource Cleanup**: Properly releases buffers and stack structures regardless of the insertion outcome

The insertdata parameter format is tree-specific (entry vs data trees) and is passed through to the appropriate callback functions without interpretation by ginInsertValue itself.

## Parameters / Member Variables
- `btree`: GinBtree structure containing tree-specific method pointers and metadata
- `stack`: GinBtreeStack representing the path from root to target insertion point  
- `insertdata`: Value to insert (format depends on tree type - entry or data)
- `buildStats`: Statistics tracking structure used during index builds (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - GinPageIsIncompleteSplit, BufferGetPage
  - [ginFinishOldSplit](ginFinishOldSplit.md), ginPlaceToPage, ginFinishSplit
  - [LockBuffer](../L/LockBuffer.md) (GIN_UNLOCK)
  - [freeGinBtreeStack](../f/freeGinBtreeStack.md)
- Called from:
  - [ginInsertItemPointers](ginInsertItemPointers.md) (src/backend/access/gin/gindatapage.c:1928)
  - [ginEntryInsert](ginEntryInsert.md) (src/backend/access/gin/gininsert.c:242)
  - [GinBtreeDataLeafInsertData](../G/GinBtreeDataLeafInsertData.md) (src/include/access/gin_private.h:209)

## Notes and Other Information
The function always consumes the passed-in stack structure, freeing it before returning (similar to freeGinBtreeStack behavior). This design simplifies caller resource management but requires callers to not reuse stack pointers after calling this function. The function handles both successful direct insertions and cases requiring splits, providing a unified interface regardless of the underlying complexity.