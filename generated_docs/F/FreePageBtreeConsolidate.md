# FreePageBtreeConsolidate

## Location
[src/backend/utils/mmgr/freepage.c:695-773](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/freepage.c#L695-L773)

## Overview
Consolidates a B-tree page with its left or right sibling when the page is less than one-third full, helping to reclaim unused pages in the free page manager's internal B-tree structure.

## Definition
static void FreePageBtreeConsolidate(FreePageManager *fpm, FreePageBtree *btp)

## Detailed Description
This function implements page consolidation logic for the free page manager's B-tree. It attempts to merge underutilized B-tree pages (those less than 1/3 full) with their siblings to prevent excessive fragmentation and maintain B-tree efficiency. The function uses a conservative approach - only consolidating pages that are significantly underutilized to avoid repeated split-merge cycles.

The consolidation process:
1. First checks if the page usage is below the 1/3 threshold
2. Attempts to merge with the right sibling by copying its keys to the current page
3. If right sibling consolidation fails, attempts to merge with the left sibling by copying current page keys to the left sibling
4. Updates parent pointers for internal pages and removes the consolidated page

The function handles both leaf and internal B-tree pages, with different key structures and consolidation logic for each type.

## Parameters / Member Variables
- : Pointer to the FreePageManager structure containing the B-tree
- : Pointer to the FreePageBtree page to be considered for consolidation

## Dependencies
- Functions called/Symbols referenced:
  - fpm_segment_base
  - FreePageBtreeFindRightSibling
  - FreePageBtreeFindLeftSibling
  - FreePageBtreeUpdateParentPointers
  - FreePageBtreeRemovePage
- Called from (representative examples):
  - FreePageBtreeRemove
  - FreePageBtreeRemovePage

## Notes and Other Information
- Only consolidates pages that are less than 1/3 full to balance efficiency with stability
- Prefers consolidating with right sibling first to avoid adjusting ancestor keys
- Handles both leaf pages (FreePageBtreeLeafKey) and internal pages (FreePageBtreeInternalKey)
- For internal page consolidation, parent pointers must be updated after key movement
- Conservative consolidation strategy prevents thrashing between split and merge operations