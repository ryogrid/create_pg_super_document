# _bt_deadblocks

## Location
src/backend/access/nbtree/nbtinsert.c: 2938 - 3010

## Overview
Builds a sorted, unique array of table block numbers from LP_DEAD-marked index tuples and the incoming newitem, optimizing simple deletion by identifying all relevant table blocks for batch processing.

## Definition
```c
static BlockNumber *_bt_deadblocks(Page page, OffsetNumber *deletable, int ndeletable, IndexTuple newitem, int *nblocks)
```

## Detailed Description
The `_bt_deadblocks` function extracts and consolidates table block numbers from index tuples that are candidates for deletion. It serves as a preprocessing step for simple deletion by creating a comprehensive list of table blocks that need to be checked for tuple deletability.

The function processes two sources of table blocks:
1. **LP_DEAD tuples**: Extracts blocks from all tuples marked as dead
2. **New item block**: Includes the block from the incoming tuple being inserted

For LP_DEAD tuples, the function handles both regular index tuples (single TID) and posting list tuples (multiple TIDs from deduplication). The array dynamically grows as needed to accommodate posting lists, which can contain multiple TIDs per tuple.

The inclusion of the newitem's table block is a performance optimization based on the observation that recently modified table blocks are likely to contain additional deletable tuples, and checking them incurs minimal cost since they're likely already in memory.

The final array is sorted and deduplicated to enable efficient binary search operations in subsequent processing steps.

## Parameters / Member Variables
- `page`: The leaf page containing the tuples to process
- `deletable`: Array of offset numbers for LP_DEAD-marked tuples
- `ndeletable`: Number of entries in the deletable array
- `newitem`: The new tuple being inserted (source of additional block)
- `nblocks`: Output parameter set to the final size of the returned array

## Dependencies
- Functions called/Symbols referenced:
  - `BTreeTupleIsPosting`: Checks if tuple contains posting list
  - `BTreeTupleIsPivot`: Validates tuple type (assertion)
  - `BTreeTupleGetNPosting`: Gets count of TIDs in posting list
  - `BTreeTupleGetPostingN`: Extracts specific TID from posting list
  - `ItemPointerGetBlockNumber`: Extracts block number from TID
  - `repalloc`: Resizes array when more space is needed
  - `qsort`: Sorts the block array for binary search efficiency
  - `qunique`: Removes duplicates from sorted array
  - `_bt_blk_cmp`: Comparison function for block number sorting
- Called from (representative examples):
  - `_bt_simpledel_pass`: To get blocks for deletion candidate identification

## Notes and Other Information
- Always includes the newitem's table block as an optimization for recent locality
- Handles posting list tuples by extracting all contained TIDs
- Uses dynamic array growth with doubling strategy for efficiency
- Ensures sufficient space allocation considering posting list expansion
- Final array is sorted and deduplicated for optimal search performance
- The newitem is validated to be neither posting nor pivot tuple type
- Returns ownership of allocated array to caller (must be freed)
- Array size optimization balances initial allocation with growth needs