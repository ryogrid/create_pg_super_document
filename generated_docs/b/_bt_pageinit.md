# _bt_pageinit

## Location
[src/backend/access/nbtree/nbtpage.c:1129-1153](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtpage.c#L1129-L1153)

## Overview
_bt_pageinit initializes a new B-tree page by setting up the page header and clearing the data and special space areas.

## Definition

```c
void
_bt_pageinit(Page page, Size size)
```
## Detailed Description
_bt_pageinit is a wrapper function that initializes a new B-tree page by calling the generic PageInit function with B-tree-specific parameters. The function sets up the standard page header structure, initializes the data space as empty, and zeros out the special space that will later contain B-tree-specific metadata.

The special space size is set to sizeof(BTPageOpaqueData), which reserves room for B-tree-specific page metadata including pointers to sibling pages, page type information, and other B-tree structural data. This initialization ensures that newly allocated pages have a consistent, clean starting state before any B-tree-specific data structures are added.

The function is used throughout the B-tree implementation whenever a fresh page needs to be created, whether during index splits, new page allocation, or WAL replay operations.

## Parameters / Member Variables
- `page`: Pointer to the page memory to be initialized
- `size`: Size of the page (typically BLCKSZ - the standard PostgreSQL block size)
## Dependencies
- Functions called/Symbols referenced:
  - [PageInit](../P/PageInit.md)
  - [BTPageOpaqueData](../B/BTPageOpaqueData.md)

- Called from (representative examples):
  - [_bt_split](_bt_split.md)
  - [_bt_initmetapage](_bt_initmetapage.md)
  - [_bt_allocbuf](_bt_allocbuf.md)
  - [_bt_blnewpage](_bt_blnewpage.md)
  - [btree_xlog_split](btree_xlog_split.md)
  - [btree_xlog_newroot](btree_xlog_newroot.md)
  - [btree_xlog_unlink_page](btree_xlog_unlink_page.md)

## Notes and Other Information
- Simple wrapper around PageInit that provides B-tree-specific initialization parameters
- Sets special space size to accommodate BTPageOpaqueData structure
- Ensures consistent initialization of all new B-tree pages across different contexts
- Used both during normal operations and WAL replay for crash recovery
- After initialization, pages are ready for B-tree-specific operations like setting page type and adding tuples
- The function does not set B-tree-specific page attributes - those are handled by subsequent operations
- Essential for maintaining B-tree page format consistency throughout the system

## Simplified Source

```c
void _bt_pageinit(Page page, Size size) {
    // Initialize page with B-tree-specific special space size
    PageInit(page, size, sizeof(BTPageOpaqueData));
}
```