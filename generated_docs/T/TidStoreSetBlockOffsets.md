# TidStoreSetBlockOffsets

## Location
src/backend/access/common/tidstore.c: 356 - 431

## Overview
Creates or replaces an entry in the TidStore for a given block number and array of offset numbers, optimized for vacuum operations.

## Definition
```c
void TidStoreSetBlockOffsets(TidStore *ts, BlockNumber blkno, OffsetNumber *offsets, int num_offsets)
```

## Detailed Description
TidStoreSetBlockOffsets creates or replaces an entry in the TidStore for the specified block number with the provided array of offset numbers. The function is specifically designed and optimized for vacuum's heap scanning phase. It supports two storage modes based on the number of offsets: for small numbers (≤ NUM_FULL_OFFSETS), it stores offsets directly in the header; for larger numbers, it uses a bitmap representation. The function validates that offset numbers are in ascending order and within valid bounds, then stores the data in either the shared or local radix tree depending on the TidStore configuration.

## Parameters / Member Variables
- `ts`: Pointer to the TidStore object
- `blkno`: Block number for which to set the offsets
- `offsets`: Array of offset numbers, must be sorted in ascending order
- `num_offsets`: Number of offsets in the array (must be > 0)

## Dependencies
- Functions called/Symbols referenced:
  - TidStoreIsShared (macro)
  - shared_ts_set (radix tree generated function)
  - local_ts_set (radix tree generated function)
  - MaxBlocktableEntrySize
  - [BlocktableEntry](../B/BlocktableEntry.md)
  - NUM_FULL_OFFSETS
  - BITS_PER_BITMAPWORD
  - WORDNUM, BITNUM, WORDS_PER_PAGE (macros)
  - InvalidOffsetNumber, MAX_OFFSET_IN_BITMAP
- Called from (representative examples):
  - [dead_items_add](../d/dead_items_add.md) (in vacuumlazy.c)
  - [do_set_block_offsets](../d/do_set_block_offsets.md) (in test_tidstore.c)

## Notes and Other Information
- The offset numbers must be sorted in ascending order
- If the block number already exists, the entry will be completely replaced (no way to add/remove individual offsets)
- Designed and optimized for vacuum's heap scanning phase
- Uses two storage strategies: direct storage for few offsets, bitmap for many offsets
- Performs bounds checking on offset numbers to prevent array overruns
- Stores data in shared or local radix tree based on TidStore configuration
- Errors if offset numbers are invalid or out of range