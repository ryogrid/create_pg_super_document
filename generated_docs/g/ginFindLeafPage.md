# ginFindLeafPage

## Location
src/backend/access/gin/ginbtree.c: 83 - 176

## Overview
ginFindLeafPage descends the GIN B-tree to locate the leaf page that contains or would contain a specified key, handling path maintenance and locking appropriately based on the operation type.

## Definition
GinBtreeStack *ginFindLeafPage(GinBtree btree, bool searchMode, bool rootConflictCheck)

## Detailed Description
ginFindLeafPage is a fundamental tree traversal function in PostgreSQL's GIN index implementation that navigates from the root to the appropriate leaf page. The function maintains a stack representing the path from root to leaf, with different locking strategies based on the operation type. In search mode, it maintains only shared locks and doesn't preserve the full path for memory efficiency. In modification mode, it maintains exclusive locks and preserves the complete path stack for potential splits. The function handles incomplete page splits encountered during traversal and supports both targeted searches and full scans.

## Parameters / Member Variables
- : GinBtree structure containing index metadata, search key, and operation-specific callbacks
- : Boolean flag - true for read-only operations, false for modification operations requiring exclusive locks
- : Boolean flag to enable serialization conflict checking at the tree root

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (memory allocation)
  - [ReadBuffer](../R/ReadBuffer.md), ReleaseAndReadBuffer (buffer management)
  - [CheckForSerializableConflictIn](../C/CheckForSerializableConflictIn.md) (serialization conflict detection)
  - [ginTraverseLock](ginTraverseLock.md) (buffer locking)
  - GinPageIsIncompleteSplit, ginFinishOldSplit (split handling)
  - GinPageGetOpaque, GinPageIsLeaf (page inspection)
  - [ginStepRight](ginStepRight.md) (rightward page navigation)
  - [LockBuffer](../L/LockBuffer.md) (buffer locking operations)
- Called from (representative examples):
  - [ginInsertItemPointers](ginInsertItemPointers.md)
  - [ginScanBeginPostingTree](ginScanBeginPostingTree.md)
  - [startScanEntry](../s/startScanEntry.md)
  - [entryLoadMoreItems](../e/entryLoadMoreItems.md)
  - [ginEntryInsert](ginEntryInsert.md)

## Notes and Other Information
The function implements an optimized traversal strategy where search operations don't maintain the full path stack to reduce memory overhead. The right-link following logic handles the case where concurrent operations may cause the search to land on the wrong page initially. The incomplete split handling ensures index consistency even in the presence of interrupted operations. The predictNumber field in the stack is used for buffer prefetching optimization during tree traversal.