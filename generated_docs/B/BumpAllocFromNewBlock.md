# BumpAllocFromNewBlock

## Location
[src/backend/utils/mmgr/bump.c:430-490](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/bump.c#L430-L490)

## Overview
BumpAllocFromNewBlock is a helper function that allocates a new memory block for the bump memory context and returns a chunk allocated from it.

## Definition

```c
static void *
BumpAllocFromNewBlock(MemoryContext context, Size size, int flags,
					  Size chunk_size)
```
## Detailed Description
This function is responsible for creating new memory blocks in the bump memory allocation context when the current block doesn't have sufficient space for the requested allocation. It implements a block size doubling strategy, starting with initBlockSize and doubling for each successive block up to maxBlockSize. The function calculates the required block size to accommodate the chunk plus necessary headers, rounds it up to the next power of 2 if needed, allocates the block using malloc, initializes it, adds it to the block list, and finally allocates the requested chunk from the new block.

## Parameters / Member Variables
- `context`: The memory context (BumpContext) requesting the new block
- `size`: The original size requested by the user
- `flags`: Memory allocation flags (e.g., MCXT_ALLOC_NO_OOM)
- `chunk_size`: The aligned size needed for the chunk (calculated by caller)
## Dependencies
- Functions called/Symbols referenced:
  - [BumpContext](BumpContext.md) (cast context to bump-specific type)
  - [BumpBlock](BumpBlock.md) (memory block structure)
  - Bump_CHUNKHDRSZ (chunk header size constant)
  - Bump_BLOCKHDRSZ (block header size constant)
  - pg_nextpower2_size_t (rounds size up to next power of 2)
  - malloc (system memory allocation)
  - [MemoryContextAllocationFailure](../M/MemoryContextAllocationFailure.md) (handles allocation failures)
  - [BumpBlockInit](BumpBlockInit.md) (initializes the new block)
  - [dlist_push_head](../d/dlist_push_head.md) (adds block to doubly-linked list)
  - [BumpAllocChunkFromBlock](BumpAllocChunkFromBlock.md) (allocates chunk from the new block)
- Called from (representative examples):
  - [BumpAlloc](BumpAlloc.md) (when current block lacks sufficient space)

## Notes and Other Information
- Marked with pg_noinline to avoid stack frame overhead in the common allocation path
- Implements exponential block size growth strategy with a maximum cap
- Always returns allocated memory directly as a tail call to BumpAllocChunkFromBlock
- Handles allocation failure through MemoryContextAllocationFailure
- Updates context's total memory allocated counter
- New blocks are added to the head of the block list for cache locality

## Simplified Source

```c
static void *
BumpAllocFromNewBlock(MemoryContext context, Size size, int flags, Size chunk_size)
{
    BumpContext *set = (BumpContext *) context;
    BumpBlock *block;
    Size blksize;

    // Double block size for next allocation, up to maximum
    blksize = set->nextBlockSize;
    set->nextBlockSize <<= 1;
    if (set->nextBlockSize > set->maxBlockSize)
        set->nextBlockSize = set->maxBlockSize;

    // Calculate required size: chunk + headers
    Size required_size = chunk_size + Bump_CHUNKHDRSZ + Bump_BLOCKHDRSZ;

    // Ensure block is large enough, round to power of 2
    if (blksize < required_size)
        blksize = pg_nextpower2_size_t(required_size);

    // Allocate and initialize new block
    block = (BumpBlock *) malloc(blksize);
    if (block == NULL)
        return MemoryContextAllocationFailure(context, size, flags);

    context->mem_allocated += blksize;
    BumpBlockInit(set, block, blksize);

    // Add to block list and allocate chunk
    dlist_push_head(&set->blocks, &block->node);
    return BumpAllocChunkFromBlock(context, block, size, chunk_size);
}
```