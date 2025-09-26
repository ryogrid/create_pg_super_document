# ltsGetFreeBlock

## Location
[src/backend/utils/sort/logtape.c:371-430](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/logtape.c#L371-L430)

## Overview
Manages allocation of free blocks from the LogicalTapeSet's global free list using a min-heap data structure to efficiently retrieve the lowest available block number.

## Definition
```c
static int64 ltsGetFreeBlock(LogicalTapeSet *lts)
```

## Detailed Description
This function implements a sophisticated block allocation strategy using a binary min-heap to track free blocks. The function follows these allocation strategies:

1. **No free blocks available**: Allocates a completely new block by incrementing `nBlocksAllocated`
2. **Single free block**: Simple case - directly returns the only free block
3. **Multiple free blocks**: Uses min-heap operations to extract the smallest available block number

When extracting from the min-heap, the function performs a standard heap deletion operation:
- Removes the root (minimum) element
- Replaces it with the last element in the heap
- Performs a "sift down" operation to restore the heap property by comparing with child nodes

## Parameters / Member Variables
- `lts`: Pointer to the LogicalTapeSet containing the free block heap and allocation counters

## Dependencies
- Functions called/Symbols referenced:
  - [left_offset](left_offset.md)
  - [right_offset](../r/right_offset.md)
  - [LogicalTapeSet](../L/LogicalTapeSet.md) (struct)
- Called from (representative examples):
  - [ltsGetBlock](ltsGetBlock.md)
  - [ltsGetPreallocBlock](ltsGetPreallocBlock.md)

## Notes and Other Information
- Returns the lowest available block number (int64), which helps minimize file fragmentation
- Uses a min-heap implementation to ensure O(log n) allocation time complexity
- The heap is stored in the `freeBlocks` array with size tracked by `nFreeBlocks`
- When no free blocks exist, it allocates sequentially by incrementing `nBlocksAllocated`
- The sift-down implementation manually handles heap property restoration after root removal
- This allocation strategy supports efficient block recycling in the logical tape system