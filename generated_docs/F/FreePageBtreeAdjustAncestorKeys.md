# FreePageBtreeAdjustAncestorKeys

## Location
[src/backend/utils/mmgr/freepage.c:501-579](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/freepage.c#L501-L579)

## Overview
Propagates key changes upward through the B-tree hierarchy when the first key of a page has been modified, maintaining B-tree invariants.

## Definition


## Detailed Description
This function maintains the critical B-tree invariant that the first_page value stored at index zero in any non-root page must match the corresponding key in its parent page. When the first key on a page changes (due to insertions, deletions, or modifications), this function walks up the ancestor chain to update all affected parent keys.

The algorithm works by:
1. Extracting the new first key from the modified page (handling both leaf and internal pages)
2. Walking up the tree, finding the parent and locating the correct index that points to the child
3. Updating the parent's key to match the child's new first key
4. Continuing upward only if the updated key was at index zero in the parent (otherwise the change doesn't propagate further)

The function assumes that the key change is small enough that it doesn't affect the ordering of keys within parent pages - it only updates the values without rearranging entries.

## Parameters / Member Variables
- : Pointer to the FreePageManager containing the B-tree
- : Pointer to the B-tree page whose first key has changed and needs ancestor updates

## Dependencies
- Functions called/Symbols referenced:
  - fpm_segment_base
  - FreePageBtree (struct type)
  - FREE_PAGE_LEAF_MAGIC, FREE_PAGE_INTERNAL_MAGIC (constants)
  - FPM_ITEMS_PER_LEAF_PAGE, FPM_ITEMS_PER_INTERNAL_PAGE (constants)
  - relptr_access
  - FreePageBtreeSearchInternal
- Called from (representative examples):
  - FreePageBtreeRemove
  - FreePageBtreeRemovePage
  - [FreePageManagerGetInternal](FreePageManagerGetInternal.md)
  - [FreePageManagerPutInternal](FreePageManagerPutInternal.md)

## Notes and Other Information
This is an internal static function critical for B-tree consistency. It includes debug assertions to verify the correctness of parent-child relationships when USE_ASSERT_CHECKING is enabled. The function handles both leaf and internal pages and performs careful index calculations to locate the correct parent key that needs updating. The upward propagation stops as soon as a non-zero index is encountered, as changes to non-first keys don't affect ancestor pages.