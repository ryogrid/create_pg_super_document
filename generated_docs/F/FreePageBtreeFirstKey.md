# FreePageBtreeFirstKey

## Location
[src/backend/utils/mmgr/freepage.c:863-879](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/freepage.c#L863-L879)

## Overview
Retrieves the first key (smallest key value) from a B-tree page, handling both leaf and internal page types.

## Definition
static Size FreePageBtreeFirstKey(FreePageBtree *btp)

## Detailed Description
This utility function extracts the first key from a B-tree page, which represents the smallest key value stored on that page. The function handles both leaf and internal pages by checking the page's magic number and accessing the appropriate key array.

For leaf pages, it returns the first_page value from the first leaf key entry. For internal pages, it returns the first_page value from the first internal key entry. The function assumes the page contains at least one key (nused > 0).

This is a critical primitive operation used throughout B-tree navigation algorithms, particularly when traversing up and down the tree to locate sibling pages or update parent references.

## Parameters / Member Variables
- : Pointer to the FreePageBtree page from which to extract the first key

## Dependencies
- Functions called/Symbols referenced:
  - FREE_PAGE_LEAF_MAGIC (constant)
  - FREE_PAGE_INTERNAL_MAGIC (constant)
- Called from (representative examples):
  - FreePageBtreeFindLeftSibling
  - FreePageBtreeFindRightSibling
  - FreePageBtreeRemovePage
  - FreePageManagerPutInternal

## Notes and Other Information
- Asserts that the page contains at least one key (nused > 0)
- Returns a Size value representing the page number of the first key
- Essential for B-tree navigation algorithms that need to identify the keyspace boundaries
- Used by sibling-finding functions to locate the current page's position within parent nodes
- The returned value represents the first_page field, which identifies the starting page number for the key's range