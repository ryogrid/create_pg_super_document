# _bt_pagestate

## Location
src/backend/access/nbtree/nbtsort.c: 646 - 682

## Overview
Allocates and initializes a new BTPageState structure that represents the working state for a B-tree page during index construction.

## Definition
```c
static BTPageState *_bt_pagestate(BTWriteState *wstate, uint32 level)
```

## Detailed Description
This function creates and initializes a new BTPageState structure that manages the state of a B-tree page during bulk loading. It allocates memory for the state structure, creates an initial page using _bt_blnewpage, assigns it a block number, and sets up various page-specific parameters including fill factor thresholds. The function distinguishes between leaf and non-leaf pages when setting the fill factor, with non-leaf pages using BTREE_NONLEAF_FILLFACTOR and leaf pages using the target page free space calculation. The state structure is immediately ready for use by _bt_buildadd for adding tuples.

## Parameters / Member Variables
- `wstate`: Pointer to BTWriteState structure containing bulk write context and allocation state
- `level`: The B-tree level for this page (0 for leaf pages, >0 for internal pages)

## Dependencies
- Functions called/Symbols referenced:
  - [BTWriteState](../B/BTWriteState.md) (parameter type)
  - [BTPageState](../B/BTPageState.md) (return type and structure being initialized)
  - [_bt_blnewpage](_bt_blnewpage.md) (to create the initial page buffer)
  - P_HIKEY (constant for high key position initialization)
  - BTREE_NONLEAF_FILLFACTOR (fill factor constant for internal pages)
  - BTGetTargetPageFreeSpace (to calculate leaf page fill threshold)
- Called from (representative examples):
  - _bt_buildadd
  - _bt_load

## Notes and Other Information
- This is a static function, only accessible within the nbtsort.c compilation unit
- Uses palloc0 to ensure the structure is zero-initialized
- Automatically assigns consecutive block numbers using btws_pages_alloced counter
- Sets btps_lastoff to P_HIKEY so the first item will be placed at P_FIRSTKEY
- Fill factor differs by level: non-leaf pages use a fixed percentage while leaf pages use dynamic calculation
- The btps_next field is initialized to NULL as parent levels are created on demand
- Part of the bulk loading infrastructure that optimizes B-tree construction by managing page state efficiently
- The btps_lowkey is initially NULL and will be set when the page receives its first tuple