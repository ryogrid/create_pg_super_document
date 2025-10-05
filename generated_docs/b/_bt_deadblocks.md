# _bt_deadblocks

## Location
[src/backend/access/nbtree/nbtinsert.c:2938-3010](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtinsert.c#L2938-L3010)

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
  - [BTreeTupleIsPosting](../B/BTreeTupleIsPosting.md): Checks if tuple contains posting list
  - [BTreeTupleIsPivot](../B/BTreeTupleIsPivot.md): Validates tuple type (assertion)
  - [BTreeTupleGetNPosting](../B/BTreeTupleGetNPosting.md): Gets count of TIDs in posting list
  - [BTreeTupleGetPostingN](../B/BTreeTupleGetPostingN.md): Extracts specific TID from posting list
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md): Extracts block number from TID
  - [repalloc](../r/repalloc.md): Resizes array when more space is needed
  - `qsort`: Sorts the block array for binary search efficiency
  - [qunique](../q/qunique.md): Removes duplicates from sorted array
  - `[_bt_blk_cmp](_bt_blk_cmp.md)`: Comparison function for block number sorting
- Called from (representative examples):
  - [_bt_simpledel_pass](_bt_simpledel_pass.md): To get blocks for deletion candidate identification

## Notes and Other Information
- Always includes the newitem's table block as an optimization for recent locality
- Handles posting list tuples by extracting all contained TIDs
- Uses dynamic array growth with doubling strategy for efficiency
- Ensures sufficient space allocation considering posting list expansion
- Final array is sorted and deduplicated for optimal search performance
- The newitem is validated to be neither posting nor pivot tuple type
- Returns ownership of allocated array to caller (must be freed)
- Array size optimization balances initial allocation with growth needs

## Simplified Source

```c
static BlockNumber *
_bt_deadblocks(Page page, OffsetNumber *deletable, int ndeletable,
              IndexTuple newitem, int *nblocks)
{
    int spacentids = ndeletable + 1;  // Initial space for dead tuples + newitem
    int ntids = 0;
    BlockNumber *tidblocks = (BlockNumber *) palloc(sizeof(BlockNumber) * spacentids);

    // Always include the newitem's table block (optimization for recent locality)
    tidblocks[ntids++] = ItemPointerGetBlockNumber(&newitem->t_tid);

    // Process all LP_DEAD marked tuples
    for (int i = 0; i < ndeletable; i++) {
        ItemId itemid = PageGetItemId(page, deletable[i]);
        IndexTuple itup = (IndexTuple) PageGetItem(page, itemid);

        if (!BTreeTupleIsPosting(itup)) {
            // Regular tuple - add its block number
            if (ntids + 1 > spacentids) {
                spacentids *= 2;
                tidblocks = (BlockNumber *) repalloc(tidblocks, sizeof(BlockNumber) * spacentids);
            }
            tidblocks[ntids++] = ItemPointerGetBlockNumber(&itup->t_tid);
        } else {
            // Posting list tuple - add all TID blocks
            int nposting = BTreeTupleGetNPosting(itup);

            if (ntids + nposting > spacentids) {
                spacentids = Max(spacentids * 2, ntids + nposting);
                tidblocks = (BlockNumber *) repalloc(tidblocks, sizeof(BlockNumber) * spacentids);
            }

            for (int j = 0; j < nposting; j++) {
                ItemPointer tid = BTreeTupleGetPostingN(itup, j);
                tidblocks[ntids++] = ItemPointerGetBlockNumber(tid);
            }
        }
    }

    // Sort and deduplicate the block array for efficient binary search
    qsort(tidblocks, ntids, sizeof(BlockNumber), _bt_blk_cmp);
    *nblocks = qunique(tidblocks, ntids, sizeof(BlockNumber), _bt_blk_cmp);

    return tidblocks;
}
```