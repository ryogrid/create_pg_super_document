# ltsReleaseBlock

## Location
[src/backend/utils/sort/logtape.c:469-521](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/logtape.c#L469-L521)

## Overview
Returns a block to the LogicalTapeSet's global free list by inserting it into a min-heap structure using a sift-up operation to maintain heap ordering.

## Definition
```c
static void ltsReleaseBlock(LogicalTapeSet *lts, int64 blocknum)
```

## Detailed Description
This function implements block deallocation for the logical tape system by returning blocks to the global free list maintained as a binary min-heap. The function handles several scenarios:

1. **Early exit conditions**: Returns immediately if the tape set has enabled `forgetFreeSpace`, indicating free space tracking is disabled
2. **Dynamic array expansion**: Doubles the free blocks array size when it becomes full, with a safety check against `MaxAllocSize`
3. **Memory leak prevention**: If the array would exceed maximum size, the function returns without adding the block (intentional leak to prevent memory exhaustion)
4. **Heap insertion**: Adds the new block using a standard min-heap sift-up operation, starting from the end and bubbling up until heap property is satisfied

The sift-up process compares the new block number with its parent nodes, moving parent values down until the correct position is found, ensuring the min-heap property is maintained.

## Parameters / Member Variables
- `lts`: Pointer to the LogicalTapeSet containing the free blocks min-heap
- `blocknum`: The block number (int64) to return to the free list

## Dependencies
- Functions called/Symbols referenced:
  - [parent_offset](../p/parent_offset.md)
  - [repalloc](../r/repalloc.md)
  - MaxAllocSize
  - [LogicalTapeSet](../L/LogicalTapeSet.md) (struct)
- Called from (representative examples):
  - [ltsReadFillBuffer](ltsReadFillBuffer.md)
  - [LogicalTapeRewindForRead](../L/LogicalTapeRewindForRead.md)

## Notes and Other Information
- Uses a min-heap data structure to ensure efficient allocation of lowest-numbered blocks
- Implements exponential growth strategy (doubling) for the free blocks array
- Includes memory safety checks to prevent excessive memory allocation
- The sift-up algorithm ensures O(log n) insertion time complexity  
- When memory limits are reached, blocks are intentionally leaked rather than causing allocation failures
- The `forgetFreeSpace` flag allows disabling free space tracking for memory-constrained scenarios
- Works in conjunction with `ltsGetFreeBlock` to provide efficient block recycling in the logical tape system

## Simplified Source

```c
static void ltsReleaseBlock(LogicalTapeSet *lts, int64 blocknum) {
    // Skip if free space tracking is disabled
    if (lts->forgetFreeSpace)
        return;

    // Expand free blocks array if needed
    if (lts->nFreeBlocks >= lts->freeBlocksLen) {
        // Prevent excessive memory usage by leaking this block
        if (lts->freeBlocksLen * 2 * sizeof(int64) > MaxAllocSize)
            return;

        // Double the array size
        lts->freeBlocksLen *= 2;
        lts->freeBlocks = (int64 *) repalloc(lts->freeBlocks,
                                           lts->freeBlocksLen * sizeof(int64));
    }

    // Insert new block into min-heap using sift-up
    int64 *heap = lts->freeBlocks;
    uint64 holepos = lts->nFreeBlocks;
    lts->nFreeBlocks++;

    // Bubble up until heap property is satisfied
    while (holepos != 0) {
        uint64 parent = parent_offset(holepos);

        if (heap[parent] < blocknum)
            break;

        heap[holepos] = heap[parent];
        holepos = parent;
    }
    heap[holepos] = blocknum;
}
```