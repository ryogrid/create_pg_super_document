# BTMetaPageData

## Location
src/include/access/nbtree.h: 103 - 119

## Overview
BTMetaPageData is the structure that defines the metadata page of a B-tree index, stored on the first page and containing essential information about the tree structure, root locations, and cleanup statistics.

## Definition


## Detailed Description
BTMetaPageData defines the structure of the metadata page that is always stored as the first page (page 0) in every B-tree index. This page serves as the entry point for all B-tree operations and contains crucial information about the tree's structure and state.

The metadata page maintains both a "root" and "fast root" concept. The regular root points to the true root of the B-tree, while the fast root is an optimization that points to the current effective root during certain operations. This dual-root system helps optimize performance during tree structure changes.

The structure also includes versioning information to support backward compatibility and upgrade scenarios, as well as statistics from the last cleanup operation to help determine when future cleanup operations are needed.

## Parameters / Member Variables
- : Magic number that should contain BTREE_MAGIC to verify the page is a valid B-tree metadata page
- : Version number of the nbtree implementation (always <= BTREE_VERSION)
- : Block number of the current root page of the B-tree
- : Tree level of the root page (height of the tree)
- : Block number of the current "fast" root location for optimization
- : Tree level of the "fast" root page
- : Number of deleted, non-recyclable pages found during the last cleanup operation
- : Number of heap tuples during last cleanup (deprecated field)
- : Boolean flag indicating whether all columns in the index are "equalimage" (support deduplication)

## Dependencies
- Functions called/Symbols referenced:
  - BlockNumber (for page references)
  - BTREE_MAGIC (magic number constant)
  - BTREE_VERSION (version constant)
  - BTREE_NOVAC_VERSION (version threshold)
- Called from (representative examples):
  - _bt_insertonpg
  - _bt_finish_split
  - _bt_newlevel
  - _bt_initmetapage
  - _bt_upgrademetapage
  - _bt_getmeta
  - _bt_vacuum_needs_cleanup
  - _bt_set_cleanup_info
  - _bt_getroot
  - _bt_gettrueroot
  - _bt_getrootheight
  - _bt_metaversion
  - _bt_unlink_halfdead_page
  - _bt_restore_meta
  - BTPageGetMeta

## Notes and Other Information
- The metadata page is always located at block 0 of every B-tree index
- Some fields are only valid when btm_version >= BTREE_NOVAC_VERSION, supporting version-specific features
- The "fast root" mechanism is an optimization described in detail in the B-tree README documentation
- The btm_last_cleanup_num_heap_tuples field is marked as deprecated and may be removed in future versions
- The btm_allequalimage flag is used to optimize deduplication operations when all indexed columns support it
- This structure is critical for index initialization, root page location, and vacuum cleanup decisions
- The metadata page format has evolved over PostgreSQL versions, with backward compatibility maintained through version checking