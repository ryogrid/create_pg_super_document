# _bt_simpledel_pass

## Location
src/backend/access/nbtree/nbtinsert.c: 2812 - 2937

## Overview
Performs simple index tuple deletion by removing LP_DEAD-marked tuples and additional safe-to-delete tuples that share table blocks with dead tuples, optimizing deletion efficiency through locality-based batching.

## Definition
```c
static void _bt_simpledel_pass(Relation rel, Buffer buffer, Relation heapRel, OffsetNumber *deletable, int ndeletable, IndexTuple newitem, OffsetNumber minoff, OffsetNumber maxoff)
```

## Detailed Description
The `_bt_simpledel_pass` function implements the simple deletion strategy for B-tree leaf pages. It not only removes tuples explicitly marked as LP_DEAD but also opportunistically identifies and removes additional tuples that can be safely deleted without significant overhead.

The key optimization is locality-based deletion: the function identifies all table blocks referenced by LP_DEAD tuples, then scans all tuples on the page to find additional tuples pointing to the same table blocks. This approach leverages the principle that if we're already checking certain table blocks for deletability, we might as well check all tuples pointing to those blocks with minimal additional cost.

The function handles both regular index tuples and posting list tuples (used in deduplication), processing all TIDs within posting lists to maximize the benefit of each table block access. The tableam (table access method) layer performs the actual deletability checks and physical deletion.

## Parameters / Member Variables
- `rel`: The index relation being modified
- `buffer`: Buffer containing the leaf page to process
- `heapRel`: The corresponding heap relation for deletability checks
- `deletable`: Array of offset numbers for LP_DEAD-marked tuples
- `ndeletable`: Number of entries in the deletable array
- `newitem`: New item being inserted (used for block analysis)
- `minoff`: First offset number to consider on the page
- `maxoff`: Last offset number to consider on the page

## Dependencies
- Functions called/Symbols referenced:
  - `_bt_deadblocks`: Gets array of table blocks pointed to by dead tuples
  - `_bt_delitems_delete_check`: Performs actual deletion after tableam validation
  - `BTreeTupleIsPosting`: Checks if tuple is a posting list tuple
  - `BTreeTupleGetNPosting`: Gets number of TIDs in posting list
  - `BTreeTupleGetPostingN`: Gets specific TID from posting list
  - `ItemPointerGetBlockNumber`: Extracts block number from TID
  - `_bt_blk_cmp`: Comparison function for binary search of blocks
- Called from (representative examples):
  - `_bt_delete_or_dedup_one_page`: As the first deletion strategy

## Notes and Other Information
- Leverages locality effects where multiple index tuples often point to the same table blocks
- Uses binary search to efficiently match TIDs against dead block arrays
- Handles posting list tuples by processing each TID individually
- May delete significantly more tuples than just those marked LP_DEAD
- The number of extra deletable tuples can greatly exceed the number of originally dead tuples
- Builds TM_IndexDeleteOp structure to interface with the tableam layer
- Allocates temporary arrays sized for maximum possible TIDs per page
- Ensures at least the originally dead tuples are included in deletion candidates