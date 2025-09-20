# AllocSetAllocFromNewBlock

## Location
[src/backend/utils/mmgr/aset.c:819-966](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/aset.c#L819-L966)

## Overview
AllocSetAllocFromNewBlock is a helper function for AllocSetAlloc() that allocates a new memory block when the current block doesn't have sufficient space, and returns a chunk allocated from the new block.

## Definition

```c
static void *
AllocSetAllocFromNewBlock(MemoryContext context, Size size, int flags,
						  int fidx)
```
## Detailed Description
This function is called when AllocSetAlloc() determines that the current active block doesn't have enough space for the requested allocation. It performs several important operations to maximize memory utilization and maintain the AllocSet data structure:

1. **Salvage remaining space**: Before creating a new block, it carves up any remaining free space in the current block into appropriately-sized chunks and adds them to the freelists for future use.

2. **Block size management**: It calculates the size for the new block using a doubling strategy (starting from initBlockSize, doubling each time up to maxBlockSize).

3. **Allocation with fallback**: It attempts to allocate the calculated block size, but includes fallback logic to try smaller sizes if the large allocation fails, down to a minimum of 1MB.

4. **Block initialization**: It properly initializes the new block's metadata, including linking it into the block list as the new active block.

5. **Chunk allocation**: Finally, it calls AllocSetAllocChunkFromBlock() to allocate the requested chunk from the new block.

The function is marked as  to prevent inlining, which helps with performance profiling and keeps the main AllocSetAlloc() function smaller.

## Parameters / Member Variables
- : The memory context requesting the allocation
- : The actual size requested by the caller (bytes)
- : Allocation flags (e.g., for error handling behavior)
- : The free list index indicating which size class this allocation belongs to

## Dependencies
- Functions called/Symbols referenced:
  - [AllocSetFreeIndex](AllocSetFreeIndex.md)
  - GetChunkSizeFromFreeListIdx  
  - [MemoryChunkSetHdrMask](../M/MemoryChunkSetHdrMask.md)
  - GetFreeListLink
  - malloc
  - [MemoryContextAllocationFailure](../M/MemoryContextAllocationFailure.md)
  - [AllocSetAllocChunkFromBlock](AllocSetAllocChunkFromBlock.md)
  - VALGRIND_MAKE_MEM_UNDEFINED
  - VALGRIND_MAKE_MEM_DEFINED
  - VALGRIND_MAKE_MEM_NOACCESS
- Called from:
  - [AllocSetAlloc](AllocSetAlloc.md)

## Notes and Other Information
- The function includes an optimization to salvage remaining space in the current block by creating appropriately-sized chunks for the freelists
- Uses a doubling strategy for block sizes to balance memory utilization and fragmentation
- Includes fallback allocation logic to handle memory pressure scenarios
- The loop that salvages space from the old block can iterate at most ALLOCSET_NUM_FREELISTS-1 times due to the ALLOC_CHUNK_LIMIT constraint
- Extensive Valgrind instrumentation helps detect memory access violations during development
- The function maintains the doubly-linked list of blocks properly by updating prev/next pointers
- Block allocation failures are handled gracefully by attempting progressively smaller block sizes