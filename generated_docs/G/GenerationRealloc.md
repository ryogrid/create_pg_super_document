# GenerationRealloc

## Location
src/backend/utils/mmgr/generation.c: 800 - 946

## Overview
GenerationRealloc implements memory reallocation for the Generation allocator, optimizing for in-place resizing when possible, otherwise performing allocation-copy-free operations.

## Definition
```c
void *GenerationRealloc(void *pointer, Size size, int flags)
```

## Detailed Description
This function handles memory reallocation within the Generation memory context with a focus on efficiency and memory safety. The implementation follows a two-path strategy:

1. **In-place Optimization**: When the existing chunk is large enough to accommodate the new size, it updates the chunk header without moving data. This is particularly efficient for size reductions and small increases.

2. **Allocate-Copy-Free Path**: When the existing chunk is too small, it:
   - Allocates a new chunk using GenerationAlloc
   - Copies existing data to the new location
   - Frees the old chunk using GenerationFree

The function includes comprehensive validation similar to GenerationFree, distinguishing between external and internal chunks for optimal performance. It also provides extensive debugging support through Valgrind integration and memory corruption detection.

Key design considerations:
- Unlike power-of-2 allocators, Generation context carves chunks to be as small as possible, meaning most realloc calls require the allocate-copy-free path
- Extensive memory access control through Valgrind annotations for debugging
- Proper handling of both growing and shrinking reallocation scenarios
- Memory safety through sentinel checking and corruption detection

## Parameters / Member Variables
- `pointer`: Pointer to the existing memory chunk to be reallocated
- `size`: New desired size in bytes
- `flags`: Allocation flags controlling behavior (e.g., MCXT_ALLOC_NO_OOM)

## Dependencies
- Functions called/Symbols referenced:
  - PointerGetMemoryChunk - converts pointer to chunk header
  - [MemoryChunkIsExternal](../M/MemoryChunkIsExternal.md) - determines chunk type
  - ExternalChunkGetBlock / MemoryChunkGetBlock - retrieves containing block
  - GenerationBlockIsValid - validates block integrity
  - [MemoryChunkGetValue](../M/MemoryChunkGetValue.md) - gets chunk size
  - [sentinel_ok](../s/sentinel_ok.md) - validates memory bounds (debug builds)
  - [randomize_mem](../r/randomize_mem.md) - fills memory with random data (debug builds)
  - [set_sentinel](../s/set_sentinel.md) - sets boundary markers (debug builds)
  - [GenerationAlloc](GenerationAlloc.md) - allocates new memory chunk
  - [MemoryContextAllocationFailure](../M/MemoryContextAllocationFailure.md) - handles allocation failures
  - [GenerationFree](GenerationFree.md) - frees the old chunk
  - memcpy - copies data between memory regions
- Called from:
  - BOGUS_MCTX - as part of memory context method table
  - General repalloc operations throughout PostgreSQL

## Notes and Other Information
- The Generation allocator's tight chunk sizing means most realloc operations require full allocate-copy-free cycles
- In-place optimization is more effective for size reductions than increases
- Extensive Valgrind integration provides detailed memory access tracking during development
- The function handles allocation failures gracefully by returning appropriate error indicators
- Memory corruption detection through sentinels helps identify buffer overruns
- Different validation approaches for external vs internal chunks balance safety with performance
- The allocate-copy-free approach ensures proper integration with the Generation allocator's block management strategy