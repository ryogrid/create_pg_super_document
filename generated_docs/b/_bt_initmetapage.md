# _bt_initmetapage

## Location
[src/backend/access/nbtree/nbtpage.c:67-106](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtpage.c#L67-L106)

## Overview
_bt_initmetapage initializes a B-tree metapage by filling a page buffer with the correct metapage structure and metadata for a B-tree index.

## Definition
void _bt_initmetapage(Page page, BlockNumber rootbknum, uint32 level, bool allequalimage)

## Detailed Description
This function initializes a B-tree metapage structure by setting up the metadata that describes the overall state of a B-tree index. The metapage is always stored at block 0 of a B-tree index and contains critical information about the tree structure, including the root page location, tree level, and various optimization flags. The function first initializes the page using _bt_pageinit, then populates the BTMetaPageData structure with essential metadata, and finally sets the page's opaque area to indicate it's a meta page. The function also correctly sets pd_lower to ensure metadata isn't lost during WAL compression.

## Parameters / Member Variables
- `page`: The page buffer to initialize as a metapage
- `rootbknum`: Block number of the B-tree root page
- `level`: Level of the B-tree root (0 for leaf level)
- `allequalimage`: Boolean flag indicating whether all tuples in the index have the same image

## Dependencies
- Functions called/Symbols referenced:
  - [_bt_pageinit](_bt_pageinit.md)
  - BTPageGetMeta
  - BTPageGetOpaque
  - BTREE_MAGIC
  - BTREE_VERSION
  - BTP_META
  - [BTMetaPageData](../B/BTMetaPageData.md)
  - BTPageOpaque
  - PageHeader
- Called from (representative examples):
  - [btbuildempty](btbuildempty.md)
  - [_bt_uppershutdown](_bt_uppershutdown.md)

## Notes and Other Information
- The metapage is critical for B-tree functionality as it contains the entry point (root page) for tree traversal
- The function sets both btm_root/btm_level and btm_fastroot/btm_fastlevel to the same values initially
- Cleanup statistics are initialized to default values (0 for deleted pages, -1.0 for heap tuples)
- The pd_lower setting is crucial to prevent metadata loss during WAL record compression
- This function is typically called during index creation or major structural changes to the B-tree