# FreePageBtreeFindRightSibling

## Location
[src/backend/utils/mmgr/freepage.c:819-862](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/freepage.c#L819-L862)

## Overview
Finds the right sibling of a given B-tree page at the same level, which is the page whose keyspace immediately follows the current page's keyspace.

## Definition
static FreePageBtree *FreePageBtreeFindRightSibling(char *base, FreePageBtree *btp)

## Detailed Description
This function navigates the B-tree structure to locate the right sibling of a specified page. It mirrors the logic of FreePageBtreeFindLeftSibling but searches in the opposite direction. The algorithm works in two phases:

1. **Ascent Phase**: Moves up the tree until it finds a parent node where the current subtree is not the rightmost child. This involves finding the first page key of the current node and checking if there's a next sibling pointer in the parent's key array.

2. **Descent Phase**: Once it can move right (to the next child pointer), it descends down the leftmost path to reach the same level as the original page.

The function handles the edge case where the passed page is the rightmost page in the entire tree level (returns NULL). It maintains proper B-tree traversal semantics using relative pointers for shared memory efficiency.

## Parameters / Member Variables
- : Base address of the shared memory segment containing the B-tree
- : Pointer to the FreePageBtree page whose right sibling should be found

## Dependencies
- Functions called/Symbols referenced:
  - FreePageBtreeFirstKey
  - relptr_access
  - FreePageBtreeSearchInternal
  - FREE_PAGE_INTERNAL_MAGIC (constant)
- Called from (representative examples):
  - FreePageBtreeConsolidate
  - FreePageManagerPutInternal

## Notes and Other Information
- Returns NULL if the passed page is the rightmost page at its level
- Uses relative pointers (relptr_access) for efficient navigation in shared memory segments
- Level tracking ensures proper descent to the correct tree depth
- During descent phase, always follows the leftmost child path (index 0) to reach the leftmost page of the right subtree
- Essential for B-tree operations requiring adjacent page access like consolidation and splitting
- The returned sibling is guaranteed to be at the same tree level as the input page