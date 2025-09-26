# FreePageBtreeFindLeftSibling

## Location
[src/backend/utils/mmgr/freepage.c:774-818](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/freepage.c#L774-L818)

## Overview
Finds the left sibling of a given B-tree page at the same level, which is the page whose keyspace immediately precedes the current page's keyspace.

## Definition
static FreePageBtree *FreePageBtreeFindLeftSibling(char *base, FreePageBtree *btp)

## Detailed Description
This function navigates the B-tree structure to locate the left sibling of a specified page. It implements a two-phase algorithm:

1. **Ascent Phase**: Moves up the tree until it finds a parent node where the current subtree is not the leftmost child. This involves finding the first page key of the current node and locating its position in the parent's key array.

2. **Descent Phase**: Once it can move left (to the previous child pointer), it descends down the rightmost path to reach the same level as the original page.

The function handles the edge case where the passed page is the leftmost page in the entire tree level (returns NULL). It uses relative pointers for navigation and maintains proper B-tree traversal semantics.

## Parameters / Member Variables
- : Base address of the shared memory segment containing the B-tree
- : Pointer to the FreePageBtree page whose left sibling should be found

## Dependencies
- Functions called/Symbols referenced:
  - [FreePageBtreeFirstKey](FreePageBtreeFirstKey.md)
  - relptr_access  
  - [FreePageBtreeSearchInternal](FreePageBtreeSearchInternal.md)
  - FREE_PAGE_INTERNAL_MAGIC (constant)
- Called from (representative examples):
  - [FreePageBtreeConsolidate](FreePageBtreeConsolidate.md)

## Notes and Other Information
- Returns NULL if the passed page is the leftmost page at its level
- Uses relative pointers (relptr_access) for memory-efficient navigation in shared memory
- Maintains level tracking to ensure proper descent to the correct tree level
- The algorithm guarantees that the returned sibling is at the same tree level as the input page
- Critical for B-tree maintenance operations like page consolidation and redistribution