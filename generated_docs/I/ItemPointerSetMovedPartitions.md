# ItemPointerSetMovedPartitions

## Location
src/include/storage/itemptr.h: 210 - 230

## Overview
Sets an ItemPointer to indicate that the referenced tuple has been moved to a different partition.

## Definition
```c
static inline void
ItemPointerSetMovedPartitions(ItemPointerData *pointer)
```

## Detailed Description
This function marks an ItemPointer with special values to indicate that the tuple it originally referenced has been moved to a different partition during an UPDATE operation. It sets the ItemPointer to contain MovedPartitionsBlockNumber (InvalidBlockNumber) and MovedPartitionsOffsetNumber (0xfffd), which serve as magic values to identify moved tuples. This is typically used to update the t_ctid field of the old tuple version when a tuple is moved to another partition.

## Parameters / Member Variables
- `pointer`: Pointer to ItemPointerData structure to modify

## Dependencies
- Functions called/Symbols referenced:
  - ItemPointerSet
  - MovedPartitionsBlockNumber
  - MovedPartitionsOffsetNumber
- Called from (representative examples):
  - HeapTupleHeaderSetMovedPartitions

## Notes and Other Information
- This function is the counterpart to ItemPointerIndicatesMovedPartitions - one sets the magic values, the other checks for them
- Uses ItemPointerSet internally to set both block number and offset number in a single operation
- Implemented as a static inline function for performance efficiency
- Essential for PostgreSQL's partition-wise UPDATE operations where tuples can move between partitions
- The magic values set by this function can be detected later using ItemPointerIndicatesMovedPartitions