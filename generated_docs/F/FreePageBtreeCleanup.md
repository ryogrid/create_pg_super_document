# FreePageBtreeCleanup

## Location
[src/backend/utils/mmgr/freepage.c:580-694](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/freepage.c#L580-L694)

## Overview
Attempts to reclaim space from the free-page B-tree by reducing tree depth and recycling unused B-tree pages, returning the size of the largest contiguous range created.

## Definition


## Detailed Description
This function performs opportunistic cleanup and optimization of the free-page B-tree structure to reduce memory overhead and potentially create larger contiguous free ranges. The cleanup operates in two main phases:

**Phase 1: Tree Depth Reduction**
- If the root has only one child, reduces tree depth by promoting the child to root
- If the root is a leaf with one entry, converts the tree to a singleton representation
- Special case: If a root leaf has two adjacent ranges that include the root page itself, merges them into a single larger range

**Phase 2: Recycled Page Reclamation**
- Attempts to return recycled B-tree pages back to the general free page pool
- Uses conservative logic to avoid counterproductive operations (skips if the reclamation would require a page split)
- Currently only attempts to reclaim the first page in the recycle list

The function tracks the largest contiguous range created during cleanup and returns this value to the caller.

## Parameters / Member Variables
- : Pointer to the FreePageManager whose B-tree should be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - fpm_segment_base
  - FreePageBtree (struct type)
  - relptr_access, relptr_store, relptr_copy
  - FREE_PAGE_LEAF_MAGIC, FREE_PAGE_INTERNAL_MAGIC (constants)
  - FreePageBtreeRecycle
  - fpm_pointer_to_page
  - [FreePagePopSpanLeader](FreePagePopSpanLeader.md), FreePagePushSpanLeader
  - FreePageBtreeGetRecycled
  - [FreePageManagerPutInternal](FreePageManagerPutInternal.md)
- Called from (representative examples):
  - FreePageManagerGet
  - [FreePageManagerPut](FreePageManagerPut.md)

## Notes and Other Information
This is an internal static function that implements an important optimization for the free page management system. The cleanup is designed to be conservative and non-disruptive - it only performs operations that are clearly beneficial. The function's logic includes special handling for the case where B-tree pages themselves can be merged into the free space they manage, creating larger contiguous ranges. The returned value helps callers understand the effectiveness of the cleanup operation.