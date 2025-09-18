# ItemPointerDec

## Location
src/backend/storage/page/itemptr.c: 114 - 131

## Overview
ItemPointerDec decrements an ItemPointer to the previous logical position, handling underflow from offset 0 to the maximum offset of the previous block, while respecting the type's range limits.

## Definition


## Detailed Description
This function implements arithmetic decrement operation for ItemPointer structures, treating them as sequential addresses within the database's physical storage. When the offset number reaches 0, the function rolls over to the maximum offset value (PG_UINT16_MAX) of the previous block number. Like ItemPointerInc, this function operates at the type level and doesn't enforce PostgreSQL's logical offset limits, only respecting the underlying data type boundaries.

The function assumes that FirstOffsetNumber is 1 rather than 0, which affects the boundary conditions. The resulting ItemPointer may become invalid according to PostgreSQL's offset numbering conventions, but this is intentional for complete type range iteration.

## Parameters / Member Variables
- : ItemPointer structure to decrement (modified in-place) - must point to valid memory

## Dependencies
- Functions called/Symbols referenced:
  - ItemPointerGetBlockNumberNoCheck: Safely extracts current block number without validation
  - ItemPointerGetOffsetNumberNoCheck: Safely extracts current offset number without validation
  - ItemPointerSet: Sets the block and offset components of the ItemPointer
  - PG_UINT16_MAX: Maximum value for 16-bit unsigned integer (used when rolling over to previous block)
- Called from (representative examples):
  - TidRangeEval: Used in TID range scan operations for backward iteration through ranges
  - ItemPointerSetMovedPartitions: Used in partition movement operations

## Notes and Other Information
- Modifies the ItemPointer in-place rather than returning a new value
- The resulting ItemPointer may have an invalid offset number according to PostgreSQL's offset conventions
- If the pointer is already at minimum values (block 0 with offset 0), no change occurs
- Assumes FirstOffsetNumber is 1, which affects boundary behavior
- Used primarily for range iteration operations requiring complete backward traversal
- Handles block number underflow by not decrementing below block 0
- The decrement operation is atomic from the caller's perspective
- Complementary function to ItemPointerInc for bidirectional ItemPointer arithmetic