# ItemPointerIndicatesMovedPartitions

## Location
src/include/storage/itemptr.h: 197 - 209

## Overview
Determines whether an ItemPointer indicates that a tuple has been moved to another partition during an UPDATE operation.

## Definition
```c
static inline bool
ItemPointerIndicatesMovedPartitions(const ItemPointerData *pointer)
```

## Detailed Description
This function checks if an ItemPointer contains special marker values that indicate a tuple has been moved to a different partition. When PostgreSQL performs an UPDATE operation that moves a tuple to a different partition, the old tuple version's t_ctid field is set to a magic value consisting of MovedPartitionsOffsetNumber (0xfffd) and MovedPartitionsBlockNumber (InvalidBlockNumber). This function tests for these specific values to identify such moved tuples.

## Parameters / Member Variables
- `pointer`: Pointer to ItemPointerData structure to examine

## Dependencies
- Functions called/Symbols referenced:
  - [ItemPointerGetOffsetNumber](ItemPointerGetOffsetNumber.md)
  - [ItemPointerGetBlockNumberNoCheck](ItemPointerGetBlockNumberNoCheck.md)
  - MovedPartitionsOffsetNumber
  - MovedPartitionsBlockNumber
- Called from (representative examples):
  - [heapam_tuple_lock](../h/heapam_tuple_lock.md)
  - [RelationFindReplTupleByIndex](../R/RelationFindReplTupleByIndex.md)
  - [RelationFindReplTupleSeq](../R/RelationFindReplTupleSeq.md)
  - [ExecOnConflictUpdate](../E/ExecOnConflictUpdate.md)
  - [ExecMergeMatched](../E/ExecMergeMatched.md)
  - HeapTupleHeaderIndicatesMovedPartitions

## Notes and Other Information
- Returns true only when both the block number equals MovedPartitionsBlockNumber (InvalidBlockNumber) and the offset number equals MovedPartitionsOffsetNumber (0xfffd)
- This is used in partition-wise operations to detect when tuples have been moved between partitions
- The function is implemented as a static inline for performance efficiency
- Part of the ItemPointer API for handling tuple location references in PostgreSQL's storage system