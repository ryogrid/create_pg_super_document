# sum_free_pages_recurse

## Location
src/backend/utils/mmgr/freepage.c: 252 - 273

## Overview
A recursive helper function that traverses the free page B-tree structure and counts all pages stored within the tree nodes.

## Definition
```c
static void sum_free_pages_recurse(FreePageManager *fpm, FreePageBtree *btp, Size *sum)
```

## Detailed Description
sum_free_pages_recurse performs a depth-first traversal of the free page B-tree to count pages used by the B-tree structure itself. This is a utility function used primarily for debugging and assertion checking to verify the consistency of internal data structures.

The function:
1. Validates that the current node has proper magic numbers (internal or leaf)
2. Increments the sum counter for the current node
3. For internal nodes, recursively visits all child nodes
4. For leaf nodes, the recursion terminates

Note that this function counts B-tree structure pages, not the actual free pages managed by the B-tree. It's used to verify that the internal accounting matches the actual structure.

## Parameters / Member Variables
- `fpm`: Pointer to the FreePageManager structure containing the base address
- `btp`: Pointer to the current B-tree node being processed
- `sum`: Pointer to accumulator variable that tracks the total count

## Dependencies
- Functions called/Symbols referenced:
  - fpm_segment_base (gets base address for relative pointer access)
  - relptr_access (accesses child nodes via relative pointers)
  - sum_free_pages_recurse (recursive self-call)
- Types/Constants referenced:
  - FreePageManager
  - FreePageBtree
  - FREE_PAGE_INTERNAL_MAGIC
  - FREE_PAGE_LEAF_MAGIC
- Called from:
  - sum_free_pages (main entry point)
  - sum_free_pages_recurse (recursive calls)

## Notes and Other Information
- This is a static function used internally within the free page manager
- The function includes assertions to validate node magic numbers for structure integrity
- Used primarily in debug builds for consistency checking via FPM_EXTRA_ASSERTS
- The recursion follows the B-tree structure from internal nodes down to leaves
- Does not count the actual free pages, only the pages used by the B-tree structure itself