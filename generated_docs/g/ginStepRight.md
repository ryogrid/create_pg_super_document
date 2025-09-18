# ginStepRight

## Location
[src/backend/access/gin/ginbtree.c:177-197](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginbtree.c#L177-L197)

## Overview
ginStepRight performs atomic rightward navigation between sibling pages in a GIN B-tree, using lock-coupling to ensure consistency during concurrent operations.

## Definition
Buffer ginStepRight(Buffer buffer, Relation index, int lockmode)

## Detailed Description
ginStepRight implements a critical page navigation operation in GIN B-trees that moves from the current page to its right sibling while maintaining proper locking discipline. The function uses lock-coupling technique where it first locks the target page before releasing the current page, preventing concurrent VACUUM operations from deleting pages during traversal. This ensures that even if VACUUM is running concurrently, the page being stepped to remains valid. The function also performs sanity checks to ensure the right sibling page has the same type (leaf/internal, entry/data) as the current page, maintaining index structure integrity.

## Parameters / Member Variables
- `buffer`: The current buffer from which to step right
- `index`: The relation (index) being traversed
- `lockmode`: The lock type to acquire on the next page (typically GIN_SHARE or GIN_EXCLUSIVE)

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](../B/BufferGetPage.md) (page retrieval from buffer)
  - GinPageIsLeaf, GinPageIsData (page type checking)
  - GinPageGetOpaque (page metadata access)
  - [ReadBuffer](../R/ReadBuffer.md) (buffer allocation and reading)
  - [LockBuffer](../L/LockBuffer.md) (buffer locking)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md) (buffer unlocking and release)
  - elog (error logging)
- Called from (representative examples):
  - [ginFindLeafPage](ginFindLeafPage.md)
  - [ginFindParents](ginFindParents.md)
  - [ginFinishSplit](ginFinishSplit.md)
  - [moveRightIfItNeeded](../m/moveRightIfItNeeded.md)
  - [scanPostingTree](../s/scanPostingTree.md)
  - [entryLoadMoreItems](../e/entryLoadMoreItems.md)

## Notes and Other Information
The lock-coupling strategy is essential for maintaining consistency in concurrent environments, particularly when VACUUM is running. Although strictly necessary only in certain scenarios, the function applies this technique universally for simplicity and safety. The page type validation at the end prevents corruption from being undetected, ensuring that index structure invariants are maintained even in edge cases. This function is fundamental to GIN B-tree traversal and is used extensively throughout the GIN access method implementation.