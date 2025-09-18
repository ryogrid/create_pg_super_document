# AllocSetRealloc

## Location
src/backend/utils/mmgr/aset.c: 1169 - 1432

## Overview
AllocSetRealloc changes the size of a previously allocated memory chunk, returning a new pointer to memory of the specified size while preserving the existing data and freeing the old memory.

## Definition


## Detailed Description
AllocSetRealloc is a sophisticated memory reallocation function that handles three distinct scenarios depending on the type and size requirements of the memory being reallocated:

1. **External chunk reallocation**: For large allocations that have their own dedicated blocks, it uses the system's realloc() function to resize the entire block in place when possible. This is the most efficient path for large allocations as it minimizes memory copying.

2. **In-place reallocation**: For regular chunks where the existing chunk is already large enough to satisfy the new size request, it simply adjusts the metadata and Valgrind annotations without moving the data. This handles both size increases (up to the chunk boundary) and decreases.

3. **Allocate-copy-free reallocation**: When a regular chunk needs to grow beyond its current size class, it allocates a new chunk, copies the existing data, and frees the old chunk. This path was simplified to avoid complex in-place expansion logic that could cause memory leaks in certain allocation patterns.

The function includes extensive memory debugging support, including detection of buffer overruns, proper Valgrind memory access tracking, and optional memory randomization. It carefully handles the transition of memory access permissions and maintains the integrity of the AllocSet data structures throughout the reallocation process.

## Parameters / Member Variables
- : Pointer to the previously allocated memory chunk to be resized
- : The new desired size for the memory allocation (in bytes)
- : Allocation control flags (e.g., MCXT_ALLOC_NO_OOM to return NULL instead of ERROR on failure)

## Dependencies
- Functions called/Symbols referenced:
  - PointerGetMemoryChunk
  - [MemoryChunkIsExternal](../M/MemoryChunkIsExternal.md)
  - ExternalChunkGetBlock
  - AllocBlockIsValid
  - MemoryContextCheckSize
  - [MemoryChunkGetBlock](../M/MemoryChunkGetBlock.md)
  - [MemoryChunkGetValue](../M/MemoryChunkGetValue.md)
  - FreeListIdxIsValid
  - GetChunkSizeFromFreeListIdx
  - MemoryChunkGetPointer
  - [AllocSetAlloc](AllocSetAlloc.md)
  - [AllocSetFree](AllocSetFree.md)
  - realloc
  - memcpy
  - [MemoryContextAllocationFailure](../M/MemoryContextAllocationFailure.md)
  - [sentinel_ok](../s/sentinel_ok.md) (when MEMORY_CONTEXT_CHECKING enabled)
  - [set_sentinel](../s/set_sentinel.md) (when MEMORY_CONTEXT_CHECKING enabled)
  - [randomize_mem](../r/randomize_mem.md) (when RANDOMIZE_ALLOCATED_MEMORY enabled)
  - Various Valgrind memory tracking macros
  - elog
- Called from:
  - BOGUS_MCTX (via function pointer)
  - Various components via the MemoryContext interface

## Notes and Other Information
- The function automatically detects whether the chunk is external or regular by examining the chunk header
- For external chunks, block pointer linkages are updated after realloc() since the block address may change
- The function avoids the complexity of in-place chunk expansion for regular chunks to prevent memory leak scenarios
- Includes comprehensive buffer overrun detection using sentinel bytes when debugging is enabled
- Handles three size relationship cases: same chunk size class, smaller request, and larger request requiring new allocation
- Memory access tracking with Valgrind is carefully maintained throughout all reallocation paths
- Without MEMORY_CONTEXT_CHECKING, some Valgrind annotations are less precise due to unknown original request size
- The allocate-copy-free path was chosen over complex in-place expansion to avoid memory leaks in palloc/repalloc/pfree cycles
- Properly handles memory accounting updates for block size changes in external chunk reallocations
- Uses conservative memory marking strategies when the original request size is unknown