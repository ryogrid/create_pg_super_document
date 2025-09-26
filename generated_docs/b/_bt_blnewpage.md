# _bt_blnewpage

## Location
[src/backend/access/nbtree/nbtsort.c:606-634](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsort.c#L606-L634)

## Overview
Allocates and initializes workspace for a new, clean B-tree page that is not linked to any sibling pages during index construction.

## Definition
```c
static BulkWriteBuffer _bt_blnewpage(BTWriteState *wstate, uint32 level)
```

## Detailed Description
This function creates a new B-tree page during the bulk loading phase of index construction. It allocates a buffer from the bulk write state, initializes the page with standard PostgreSQL page header information, and sets up B-tree specific opaque data. The page is created as an isolated page with no sibling links (btpo_prev and btpo_next set to P_NONE), making it suitable for initial page creation during index building. The function also handles the special case of reserving space for the high key (P_HIKEY) by adjusting the page's lower boundary.

## Parameters / Member Variables
- `wstate`: Pointer to BTWriteState structure containing bulk write context and state information
- `level`: The B-tree level where this page will be placed (0 for leaf pages, >0 for internal pages)

## Dependencies
- Functions called/Symbols referenced:
  - [BTWriteState](../B/BTWriteState.md) (parameter type)
  - BulkWriteBuffer (return type and local variable)
  - BTPageOpaque (local variable type)
  - [smgr_bulk_get_buf](../s/smgr_bulk_get_buf.md) (to allocate buffer from bulk state)
  - [_bt_pageinit](_bt_pageinit.md) (to initialize basic page structure)
  - BTPageGetOpaque (to access B-tree opaque data)
  - P_NONE (constant for unlinked page pointers)
  - BTP_LEAF (flag for leaf pages)
  - PageHeader (for page header manipulation)
  - [ItemIdData](../I/ItemIdData.md) (for size calculation)
- Called from (representative examples):
  - [_bt_pagestate](_bt_pagestate.md)
  - [_bt_buildadd](_bt_buildadd.md)

## Notes and Other Information
- This is a static function, only accessible within the nbtsort.c compilation unit
- Creates unlinked pages (btpo_prev = btpo_next = P_NONE) suitable for initial construction
- Automatically sets BTP_LEAF flag for level 0 pages, leaves internal pages unflagged
- Reserves space for P_HIKEY by incrementing pd_lower, ensuring the high key slot is marked as allocated
- Part of the bulk loading infrastructure that optimizes B-tree index creation performance
- The btpo_cycleid is initialized to 0, which is appropriate for newly created pages