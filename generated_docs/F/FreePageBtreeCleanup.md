# FreePageBtreeCleanup

## Location
[src/backend/utils/mmgr/freepage.c:580-694](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/freepage.c#L580-L694)

## Overview
Attempts to reclaim space from the free-page B-tree by reducing tree depth and recycling unused B-tree pages, returning the size of the largest contiguous range created.

## Definition

```c
static Size
FreePageBtreeCleanup(FreePageManager *fpm)
```
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
- `*fpm`: Pointer to the FreePageManager whose B-tree should be cleaned up
## Dependencies
- Functions called/Symbols referenced:
  - fpm_segment_base
  - [FreePageBtree](FreePageBtree.md) (struct type)
  - relptr_access, relptr_store, relptr_copy
  - FREE_PAGE_LEAF_MAGIC, FREE_PAGE_INTERNAL_MAGIC (constants)
  - [FreePageBtreeRecycle](FreePageBtreeRecycle.md)
  - fpm_pointer_to_page
  - [FreePagePopSpanLeader](FreePagePopSpanLeader.md), FreePagePushSpanLeader
  - [FreePageBtreeGetRecycled](FreePageBtreeGetRecycled.md)
  - [FreePageManagerPutInternal](FreePageManagerPutInternal.md)
- Called from (representative examples):
  - [FreePageManagerGet](FreePageManagerGet.md)
  - [FreePageManagerPut](FreePageManagerPut.md)

## Notes and Other Information
This is an internal static function that implements an important optimization for the free page management system. The cleanup is designed to be conservative and non-disruptive - it only performs operations that are clearly beneficial. The function's logic includes special handling for the case where B-tree pages themselves can be merged into the free space they manage, creating larger contiguous ranges. The returned value helps callers understand the effectiveness of the cleanup operation.

## Simplified Source

```c
static Size FreePageBtreeCleanup(FreePageManager *fpm)
{
    char *base = fpm_segment_base(fpm);
    Size max_contiguous_pages = 0;

    // Phase 1: Attempt to shrink btree depth
    while (!relptr_is_null(fpm->btree_root)) {
        FreePageBtree *root = relptr_access(base, fpm->btree_root);

        // If root has only one key, reduce depth
        if (root->hdr.nused == 1) {
            fpm->btree_depth--;

            if (root->hdr.magic == FREE_PAGE_LEAF_MAGIC) {
                // Convert leaf to singleton
                relptr_store(base, fpm->btree_root, NULL);
                fpm->singleton_first_page = root->u.leaf_key[0].first_page;
                fpm->singleton_npages = root->u.leaf_key[0].npages;
            } else {
                // Promote child to root
                relptr_copy(fpm->btree_root, root->u.internal_key[0].child);
                FreePageBtree *newroot = relptr_access(base, fpm->btree_root);
                relptr_store(base, newroot->hdr.parent, NULL);
            }
            FreePageBtreeRecycle(fpm, fmp_pointer_to_page(base, root));
        }
        // Special case: merge two adjacent ranges including root page
        else if (root->hdr.nused == 2 && root->hdr.magic == FREE_PAGE_LEAF_MAGIC) {
            Size end_first = root->u.leaf_key[0].first_page + root->u.leaf_key[0].npages;
            Size start_second = root->u.leaf_key[1].first_page;

            // Check if ranges are adjacent and include root page
            if (end_first + 1 == start_second &&
                end_first == fmp_pointer_to_page(base, root)) {
                // Merge ranges and include root page
                FreePagePopSpanLeader(fpm, root->u.leaf_key[0].first_page);
                FreePagePopSpanLeader(fpm, root->u.leaf_key[1].first_page);

                fpm->singleton_first_page = root->u.leaf_key[0].first_page;
                fpm->singleton_npages = root->u.leaf_key[0].npages +
                                      root->u.leaf_key[1].npages + 1;
                fpm->btree_depth = 0;
                relptr_store(base, fpm->btree_root, NULL);

                FreePagePushSpanLeader(fpm, fpm->singleton_first_page,
                                     fpm->singleton_npages);
                max_contiguous_pages = fpm->singleton_npages;
            }
            break;
        } else {
            break;  // Nothing more to do
        }
    }

    // Phase 2: Reclaim recycled btree pages
    while (fpm->btree_recycle_count > 0) {
        FreePageBtree *btp = FreePageBtreeGetRecycled(fpm);
        Size first_page = fmp_pointer_to_page(base, btp);

        Size contiguous_pages = FreePageManagerPutInternal(fpm, first_page, 1, true);
        if (contiguous_pages == 0) {
            // Put it back if we can't reclaim without splits
            FreePageBtreeRecycle(fpm, first_page);
            break;
        } else {
            if (contiguous_pages > max_contiguous_pages)
                max_contiguous_pages = contiguous_pages;
        }
    }

    return max_contiguous_pages;
}
```