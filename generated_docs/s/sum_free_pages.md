# sum_free_pages

## Location
src/backend/utils/mmgr/freepage.c: 274 - 323

## Overview
Calculates the total number of free pages managed by a FreePageManager by traversing and summing all pages in freelists, B-tree structures, and recycle lists.

## Definition
```c
static Size sum_free_pages(FreePageManager *fpm)
```

## Detailed Description
sum_free_pages provides a comprehensive count of all free pages managed by the free page manager. It serves as a verification function primarily used in debug builds to ensure that internal accounting matches the actual data structures.

The function counts pages from three sources:
1. **Freelists**: Traverses all FPM_NUM_FREELISTS freelists and sums the npages field from each span
2. **B-tree internal pages**: If a B-tree exists (depth > 0), recursively counts pages used by the B-tree structure itself
3. **Recycle list**: Counts single-page entries in the B-tree recycle list

This function is essential for maintaining data structure integrity and is used in assertions to verify that the cached free_pages count matches the actual structure contents.

## Parameters / Member Variables
- `fpm`: Pointer to the FreePageManager structure to analyze

## Dependencies
- Functions called/Symbols referenced:
  - fpm_segment_base (gets base address for relative pointer operations)
  - relptr_access (accesses linked list nodes via relative pointers)
  - sum_free_pages_recurse (recursively counts B-tree pages)
- Types/Constants referenced:
  - FreePageManager
  - FreePageSpanLeader
  - FreePageBtree
  - FPM_NUM_FREELISTS
- Called from:
  - FreePageManagerGet (debug assertion)
  - FreePageManagerPut (debug assertion)

## Notes and Other Information
- This is a static function used internally for debugging and verification
- The function includes an assertion that recycle list entries are always single pages
- Used primarily in debug builds via FPM_EXTRA_ASSERTS macro
- Provides comprehensive verification by checking all data structures that hold free pages
- The B-tree page counting includes internal structure pages, not just the managed free pages
- Performance is O(n) where n is the total number of free page spans and B-tree nodes