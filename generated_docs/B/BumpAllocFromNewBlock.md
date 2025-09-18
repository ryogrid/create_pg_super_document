# BumpAllocFromNewBlock

## Location
src/backend/utils/mmgr/bump.c: 430 - 490

## Overview
BumpAllocFromNewBlock is a helper function that allocates a new memory block for the bump memory context and returns a chunk allocated from it.

## Definition


## Detailed Description
This function is responsible for creating new memory blocks in the bump memory allocation context when the current block doesn't have sufficient space for the requested allocation. It implements a block size doubling strategy, starting with initBlockSize and doubling for each successive block up to maxBlockSize. The function calculates the required block size to accommodate the chunk plus necessary headers, rounds it up to the next power of 2 if needed, allocates the block using malloc, initializes it, adds it to the block list, and finally allocates the requested chunk from the new block.

## Parameters / Member Variables
- : The memory context (BumpContext) requesting the new block
- : The original size requested by the user
- : Memory allocation flags (e.g., MCXT_ALLOC_NO_OOM)
- : The aligned size needed for the chunk (calculated by caller)

## Dependencies
- Functions called/Symbols referenced:
  - BumpContext (cast context to bump-specific type)
  - BumpBlock (memory block structure)
  - Bump_CHUNKHDRSZ (chunk header size constant)
  - Bump_BLOCKHDRSZ (block header size constant)
  - pg_nextpower2_size_t (rounds size up to next power of 2)
  - malloc (system memory allocation)
  - MemoryContextAllocationFailure (handles allocation failures)
  - BumpBlockInit (initializes the new block)
  - dlist_push_head (adds block to doubly-linked list)
  - BumpAllocChunkFromBlock (allocates chunk from the new block)
- Called from (representative examples):
  - BumpAlloc (when current block lacks sufficient space)

## Notes and Other Information
- Marked with pg_noinline to avoid stack frame overhead in the common allocation path
- Implements exponential block size growth strategy with a maximum cap
- Always returns allocated memory directly as a tail call to BumpAllocChunkFromBlock
- Handles allocation failure through MemoryContextAllocationFailure
- Updates context's total memory allocated counter
- New blocks are added to the head of the block list for cache locality