# GenerationAllocFromNewBlock

## Location
[src/backend/utils/mmgr/generation.c:461-526](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/generation.c#L461-L526)

## Overview
A helper function for GenerationAlloc() that allocates a new memory block and returns a chunk allocated from it, implementing the generation memory context's block allocation strategy with progressive size doubling.

## Definition


## Detailed Description
GenerationAllocFromNewBlock is a specialized allocation function that handles the creation of new memory blocks when the current block cannot satisfy an allocation request. The function implements a progressive block sizing strategy where each new block doubles in size (up to a maximum limit) to reduce fragmentation and improve allocation efficiency. It performs the complete workflow of calculating block size, allocating system memory, initializing the block structure, linking it to the context's block list, and finally allocating the requested chunk from the new block.

The function uses a doubling strategy for block sizes, starting with initBlockSize and doubling with each allocation until reaching maxBlockSize. This approach balances memory efficiency with allocation performance by reducing the frequency of system malloc calls for larger working sets.

## Parameters / Member Variables
- : The memory context requesting the allocation (cast to GenerationContext)
- : The original size requested by the caller
- : Allocation flags that control error handling behavior
- : The actual chunk size to allocate (may be larger than size due to alignment)

## Dependencies
- Functions called/Symbols referenced:
  - pg_nextpower2_size_t (rounds size up to next power of 2)
  - malloc (system memory allocation)
  - [MemoryContextAllocationFailure](../M/MemoryContextAllocationFailure.md) (handles allocation failures)
  - [GenerationBlockInit](GenerationBlockInit.md) (initializes the new block structure)
  - [dlist_push_head](../d/dlist_push_head.md) (adds block to the context's block list)
  - [GenerationAllocChunkFromBlock](GenerationAllocChunkFromBlock.md) (allocates the actual chunk)
- Called from (representative examples):
  - [GenerationAlloc](GenerationAlloc.md) (when current block cannot satisfy request)

## Notes and Other Information
- Marked as pg_noinline to keep GenerationAlloc() lean for the common case
- Implements exponential block size growth with configurable maximum
- Handles allocation failure by delegating to MemoryContextAllocationFailure
- Updates context's total allocated memory tracking
- Makes the newly created block the current active block
- Block size calculation ensures sufficient space for chunk header, block header, and actual data