# ItemPointerSetBlockNumber

## Location
[src/include/storage/itemptr.h:147-157](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/itemptr.h#L147-L157)

## Overview
Sets only the block number portion of a disk item pointer, leaving the offset number unchanged, providing targeted modification of the block location component.

## Definition

```c
static inline void
ItemPointerSetBlockNumber(ItemPointerData *pointer, BlockNumber blockNumber)
```
## Detailed Description
ItemPointerSetBlockNumber is a specialized inline function that modifies only the block number component of an existing ItemPointerData structure while preserving the current offset number. This function is particularly useful when relocating tuples between blocks but maintaining their relative position within the destination block, or when updating references during block splits, merges, or other reorganization operations.

The function provides a focused interface for block-level updates without affecting intra-block positioning, making it ideal for scenarios where only the block reference needs to change. It maintains the same validation and delegation pattern as ItemPointerSet but operates on a subset of the item pointer components.

## Parameters / Member Variables
- `*pointer`: Pointer to the ItemPointerData structure to be modified (must be valid)
- `blockNumber`: The new block number to assign to the item pointer
## Dependencies
- Functions called/Symbols referenced:
  - PointerIsValid (assertion validation)
  - [BlockIdSet](../B/BlockIdSet.md) (sets the block identifier portion)
- Called from (representative examples):
  - [gistplacetopage](../g/gistplacetopage.md)
  - [heap_xlog_delete](../h/heap_xlog_delete.md)
  - GinItemPointerSetBlockNumber
  - [BTreeTupleSetPosting](../B/BTreeTupleSetPosting.md)
  - [BTreeTupleSetDownLink](../B/BTreeTupleSetDownLink.md)

## Notes and Other Information
- This is an inline function defined in itemptr.h for optimal performance
- Preserves the existing offset number while updating only the block component
- Commonly used in index operations and WAL replay scenarios
- Essential for tuple relocation operations during page splits and maintenance
- Provides atomic block number updates without affecting offset positioning