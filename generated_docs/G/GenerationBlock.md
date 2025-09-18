# GenerationBlock

## Location
src/backend/utils/mmgr/generation.c: 87 - 101

## Overview
GenerationBlock represents the fundamental unit of memory allocation in the generation memory context, containing zero or more memory chunks that are allocated and freed as a cohesive unit.

## Definition


## Detailed Description
GenerationBlock is the header structure for memory blocks managed by the generation memory allocator. Each block is obtained from malloc() and contains zero or more MemoryChunks, which are the individual allocations requested by palloc() and freed by pfree(). The key design principle is that individual chunks cannot be returned to malloc() separately; instead, pfree() updates the free counter, and only when all chunks in a block are freed can the entire block be returned to malloc().

The block header contains metadata to track the block's size, the number of allocated and free chunks, and pointers to manage the free space within the block. The usable space for memory chunks begins at the next alignment boundary after the GenerationBlock header.

## Parameters / Member Variables
- : Doubly-linked list node for maintaining the block in the context's block list
- : Pointer back to the GenerationContext that owns this block
- : The total allocated size of this block (including the header)
- : The current number of allocated chunks within this block
- : The number of chunks that have been freed but not yet reclaimed
- : Pointer to the start of available free space within the block for new allocations
- : Pointer to the end of the usable space in this block

## Dependencies
- Functions called/Symbols referenced:
  - dlist_node
  - GenerationContext
- Called from (representative examples):
  - Generation_BLOCKHDRSZ
  - ExternalChunkGetBlock
  - KeeperBlock
  - IsKeeperBlock
  - GenerationContextCreate
  - GenerationReset
  - GenerationAllocLarge
  - GenerationAllocChunkFromBlock
  - GenerationAllocFromNewBlock
  - GenerationAlloc
  - GenerationBlockInit
  - GenerationBlockMarkEmpty
  - GenerationBlockFreeBytes
  - GenerationBlockFree
  - GenerationFree
  - GenerationRealloc
  - GenerationGetChunkContext
  - GenerationGetChunkSpace
  - GenerationIsEmpty
  - GenerationStats
  - GenerationCheck

## Notes and Other Information
- The block header is followed by usable memory space aligned to the next alignment boundary
- Blocks can only be freed when all chunks within them have been freed (nfree equals nchunks)
- The generation allocator does not reuse freed chunks within a block; it only allocates from the free space at the end
- Block sizes can vary within a context, controlled by the GenerationContext parameters
- Part of PostgreSQL's memory management system designed for allocation patterns with clear generational behavior
- Located in src/backend/utils/mmgr/generation.c as part of the generation memory allocator implementation
- The freeptr and endptr track the allocation frontier within each block for efficient linear allocation