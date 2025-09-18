# AllocSetAlloc

## Location
src/backend/utils/mmgr/aset.c: 967 - 1061

## Overview
AllocSetAlloc is the main allocation function for the AllocSet memory context, returning a pointer to allocated memory of the given size or handling allocation failures based on the provided flags.

## Definition


## Detailed Description
AllocSetAlloc is the primary allocation function that implements a sophisticated multi-tier allocation strategy optimized for performance and memory efficiency. The function follows a carefully designed decision tree:

1. **Large allocation handling**: Requests exceeding the allocChunkLimit are delegated to AllocSetAllocLarge() for special handling.

2. **Freelist reuse**: For smaller allocations, it first checks the appropriate freelist to see if a previously freed chunk of the right size can be reused. This is the fastest allocation path.

3. **Current block allocation**: If no suitable freed chunk exists, it tries to allocate from the current active block using AllocSetAllocChunkFromBlock().

4. **New block allocation**: If the current block lacks sufficient space, it calls AllocSetAllocFromNewBlock() to create a new block.

The function is heavily optimized for performance since memory allocation is often a bottleneck. It keeps common code paths inline while delegating complex cases to pg_noinline helper functions to avoid stack frame overhead in the common cases.

The function includes extensive debugging support through Valgrind instrumentation and optional memory context checking features.

## Parameters / Member Variables
- : The AllocSet memory context from which to allocate
- : The number of bytes to allocate  
- : Allocation control flags (e.g., MCXT_ALLOC_NO_OOM to return NULL instead of ERROR on failure)

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetIsValid
  - [AllocSetAllocLarge](AllocSetAllocLarge.md)
  - [AllocSetFreeIndex](AllocSetFreeIndex.md)
  - GetFreeListLink
  - [MemoryChunkGetValue](../M/MemoryChunkGetValue.md)
  - GetChunkSizeFromFreeListIdx
  - [set_sentinel](../s/set_sentinel.md) (when MEMORY_CONTEXT_CHECKING enabled)
  - [randomize_mem](../r/randomize_mem.md) (when RANDOMIZE_ALLOCATED_MEMORY enabled)  
  - MemoryChunkGetPointer
  - [AllocSetAllocFromNewBlock](AllocSetAllocFromNewBlock.md)
  - [AllocSetAllocChunkFromBlock](AllocSetAllocChunkFromBlock.md)
  - VALGRIND_MAKE_MEM_DEFINED
  - VALGRIND_MAKE_MEM_NOACCESS
- Called from:
  - [AllocSetRealloc](AllocSetRealloc.md)
  - BOGUS_MCTX (via function pointer)
  - Various components via the MemoryContext interface

## Notes and Other Information
- The function is designed to handle all allocation sizes up to MAXALIGN_DOWN(SIZE_MAX) - ALLOC_BLOCKHDRSZ - ALLOC_CHUNKHDRSZ, though practical limits are much lower
- Includes an optimization to avoid always reserving space for sentinel bytes in power-of-2 allocations
- Uses the  compiler hint to optimize the common case where the current block has sufficient space
- Freelist chunks are maintained with their headers marked NOACCESS until allocation, providing memory debugging benefits
- The function carefully manages Valgrind memory access tracking to help detect memory violations
- Returns space that may be marked NOACCESS in some code paths, which AllocSetRealloc must account for
- Performance-critical code paths are kept inline while complex cases are delegated to helper functions