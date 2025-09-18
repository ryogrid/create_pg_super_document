# _bt_pagedel

## Location
src/backend/access/nbtree/nbtpage.c: 1802 - 2087

## Overview
This function performs the complete deletion of a leaf page from a B-tree index, coordinating both the marking phase and unlinking phase of page deletion while maintaining index integrity.

## Definition


## Detailed Description
_bt_pagedel is the main entry point for B-tree leaf page deletion, implementing a complete two-phase deletion process. The function first marks empty leaf pages as half-dead (removing downlinks from parent pages), then progressively unlinks pages from their siblings until the entire subtree is deleted.

The function handles several complex scenarios including:
- Detection and handling of incomplete splits that would make deletion unsafe
- Iterative deletion of right siblings when they become deletable after removing downlinks
- Cooperation with VACUUM bulk delete statistics to avoid double-counting deleted pages
- Recovery from interrupted deletion operations (pages already marked half-dead)

The algorithm maintains strict safety checks to prevent deletion of rightmost pages, root pages, non-empty pages, and pages involved in incomplete splits. It uses a search stack to locate parent pages and coordinate the hierarchical deletion process.

## Parameters
- : The B-tree index relation being modified
- : Buffer containing the target leaf page to delete (must be pinned and locked)
- : VACUUM state containing bulk delete statistics and heap relation reference

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md) (gets block number for tracking)
  - BTPageGetOpaque (accesses B-tree page metadata)
  - P_ISLEAF, P_ISDELETED, P_ISHALFDEAD (page state checks)
  - P_RIGHTMOST, P_ISROOT, P_INCOMPLETE_SPLIT (safety checks)
  - [_bt_leftsib_splitflag](_bt_leftsib_splitflag.md) (checks for incomplete split conditions)
  - [CopyIndexTuple](../C/CopyIndexTuple.md) (creates copy of high key for search)
  - [_bt_mkscankey](_bt_mkscankey.md) (creates insertion scan key)
  - [_bt_search](_bt_search.md) (finds parent page location)
  - [_bt_mark_page_halfdead](_bt_mark_page_halfdead.md) (first phase: marks page and removes downlinks)
  - [_bt_unlink_halfdead_page](_bt_unlink_halfdead_page.md) (second phase: unlinks pages from siblings)
  - [_bt_getbuf](_bt_getbuf.md), _bt_relbuf, _bt_lockbuf, _bt_unlockbuf (buffer management)
- Called from:
  - [btvacuumpage](btvacuumpage.md) (main VACUUM page processing loop)

## Notes and Other Information
- Implements complete page deletion with two distinct phases for crash recovery
- Can delete multiple adjacent empty pages in a single call by following right siblings
- Maintains VACUUM bulk delete statistics cooperation to avoid double-counting
- Uses temporary memory context due to memory leakage during complex operations
- Includes extensive safety checks to prevent deletion in unsafe conditions
- Handles legacy half-dead internal pages from pre-9.4 PostgreSQL versions
- The function may iterate multiple times when processing chains of deletable siblings
- Drops and reacquires locks strategically to avoid deadlocks during parent page searches