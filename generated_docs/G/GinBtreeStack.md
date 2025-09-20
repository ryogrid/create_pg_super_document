# GinBtreeStack

## Location
[src/include/access/gin_private.h:129-138](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/gin_private.h#L129-L138)

## Overview
GinBtreeStack is a stack structure used during GIN B-tree traversal operations, maintaining the path from root to current position with parent-child relationships and position tracking.

## Definition

```c
typedef struct GinBtreeStack
{
	BlockNumber blkno;
	Buffer		buffer;
	OffsetNumber off;
	ItemPointerData iptr;
	/* predictNumber contains predicted number of pages on current level */
	uint32		predictNumber;
	struct GinBtreeStack *parent;
} GinBtreeStack;
```
## Detailed Description
GinBtreeStack implements a linked stack data structure that tracks the traversal path through a GIN B-tree during search and modification operations. Each stack entry represents a level in the tree hierarchy, maintaining both the physical location (block number, buffer) and logical position (offset, item pointer) within that level. The stack enables efficient navigation and backtracking during tree operations, particularly important for split operations and parent updates.

## Parameters / Member Variables
- : Block number of the page at this stack level
- : Buffer containing the page data for this level
- : Offset number indicating position within the page
- : Item pointer data for precise tuple location
- : Predicted number of pages at the current tree level (used for optimization)
- : Pointer to parent stack entry, forming the linked stack structure

## Dependencies
- Functions called/Symbols referenced:
  - [GinBtreeStack](GinBtreeStack.md) (self-reference for parent pointer)
- Called from (representative examples):
  - [ginFindLeafPage](../g/ginFindLeafPage.md)
  - [ginFindParents](../g/ginFindParents.md)
  - [ginPlaceToPage](../g/ginPlaceToPage.md)
  - [ginFinishSplit](../g/ginFinishSplit.md)
  - [dataLocateItem](../d/dataLocateItem.md)
  - [entryLocateEntry](../e/entryLocateEntry.md)

## Notes and Other Information
- Located in src/include/access/gin_private.h:129-138
- Forms a linked list structure through the parent pointer
- Essential for maintaining tree traversal state during complex operations
- Used extensively in ginbtree.c for B-tree navigation functions
- The predictNumber field helps optimize page allocation decisions
- Memory management handled by freeGinBtreeStack function