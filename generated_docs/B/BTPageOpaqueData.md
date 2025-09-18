# BTPageOpaqueData

## Location
[src/include/access/nbtree.h:62-69](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/nbtree.h#L62-L69)

## Overview
BTPageOpaqueData is a structure stored at the end of every B-tree page in PostgreSQL that contains metadata for page navigation, level information, and vacuum cycle tracking to support concurrent operations and recovery.

## Definition


## Detailed Description
BTPageOpaqueData is a critical structure in PostgreSQL's B-tree implementation that provides essential metadata for each B-tree page. This structure is stored in the special area at the end of every B-tree page and serves multiple purposes:

1. **Page Navigation**: Contains pointers to left and right sibling pages to support forward/backward index scans and concurrent page split/deletion recovery.
2. **Tree Level Tracking**: Stores the page's level in the B-tree hierarchy (0 for leaf pages, increasing upward).
3. **Page Status Management**: Uses flag bits to indicate page type and status.
4. **Vacuum Cycle Tracking**: Maintains a cycle ID to help VACUUM detect whether a page was split during processing.

The structure is essential for recovery when searches navigate to wrong pages due to concurrent operations, as detailed in the B-tree README documentation.

## Parameters / Member Variables
- : Block number of the left sibling page, or P_NONE if this is the leftmost page at this level
- : Block number of the right sibling page, or P_NONE if this is the rightmost page at this level  
- : Tree level of this page, with 0 indicating leaf pages and higher values for internal pages
- : Bit flags indicating page type and status (e.g., BTP_LEAF, BTP_ROOT, BTP_DELETED, etc.)
- : Vacuum cycle identifier used to detect page splits during VACUUM operations

## Dependencies
- Functions called/Symbols referenced:
  - BTCycleId (typedef for vacuum cycle tracking)
  - BlockNumber (for page references)
- Called from (representative examples):
  - _bt_singleval_fillfactor
  - [_bt_checkpage](../b/_bt_checkpage.md)
  - [_bt_pageinit](../b/_bt_pageinit.md)
  - [_bt_findsplitloc](../b/_bt_findsplitloc.md)
  - BTPageOpaque (macro accessor)
  - BTMaxItemSize
  - BTMaxItemSizeNoHeapTid
  - MaxTIDsPerBTreePage

## Notes and Other Information
- The BTP_LEAF flag bit is technically redundant since level==0 could be tested instead for leaf pages
- The btpo_level field was historically a union to allow deleted pages to store 32-bit safexid values, but this has been replaced with BTDeletedPageData for 64-bit safexid storage
- During page splits, the BTP_SPLIT_END flag is managed to indicate split boundaries and help with concurrent operation recovery
- The vacuum cycle ID mechanism has a small probability of false matches if a page was split exactly MAX_BT_CYCLE_ID VACUUMs ago
- This structure is fundamental to B-tree concurrency control and is referenced throughout the nbtree access method implementation