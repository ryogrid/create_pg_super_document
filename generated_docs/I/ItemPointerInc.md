# ItemPointerInc

## Location
[src/backend/storage/page/itemptr.c:84-113](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/page/itemptr.c#L84-L113)

## Overview
ItemPointerInc increments an ItemPointer to the next logical position, handling overflow from maximum offset to the next block, while respecting the type's range limits.

## Definition


## Detailed Description
This function implements arithmetic increment operation for ItemPointer structures, treating them as sequential addresses within the database's physical storage. When the offset number reaches its maximum value (PG_UINT16_MAX), the function rolls over to offset 0 of the next block number. The function operates at the type level, meaning it doesn't enforce PostgreSQL's logical offset limits (FirstOffsetNumber/MaxOffsetNumber) but only respects the underlying data type boundaries.

The function handles edge cases where the ItemPointer might become invalid according to PostgreSQL's offset numbering conventions, but this is intentional for operations that need to iterate through the entire type range.

## Parameters / Member Variables
- : ItemPointer structure to increment (modified in-place) - must point to valid memory

## Dependencies
- Functions called/Symbols referenced:
  - [ItemPointerGetBlockNumberNoCheck](ItemPointerGetBlockNumberNoCheck.md): Safely extracts current block number without validation
  - [ItemPointerGetOffsetNumberNoCheck](ItemPointerGetOffsetNumberNoCheck.md): Safely extracts current offset number without validation
  - [ItemPointerSet](ItemPointerSet.md): Sets the block and offset components of the ItemPointer
  - PG_UINT16_MAX: Maximum value for 16-bit unsigned integer (used for offset overflow detection)
- Called from (representative examples):
  - [TidRangeEval](../T/TidRangeEval.md): Used in TID range scan operations for advancing through ranges
  - [ItemPointerSetMovedPartitions](ItemPointerSetMovedPartitions.md): Used in partition movement operations

## Notes and Other Information
- Modifies the ItemPointer in-place rather than returning a new value
- The resulting ItemPointer may have an invalid offset number according to PostgreSQL's offset conventions
- If the pointer is already at maximum values (InvalidBlockNumber with PG_UINT16_MAX offset), no change occurs
- Used primarily for range iteration operations where complete type range coverage is needed
- Handles block number overflow by not incrementing beyond InvalidBlockNumber
- The increment operation is atomic from the caller's perspective