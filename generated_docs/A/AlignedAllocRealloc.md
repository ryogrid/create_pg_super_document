# AlignedAllocRealloc

## Location
src/backend/utils/mmgr/alignedalloc.c: 61 - 135

## Overview
Resizes an aligned memory allocation while preserving the original alignment boundary, copying existing data to the new allocation and freeing the old one.

## Definition
```c
void *AlignedAllocRealloc(void *pointer, Size size, int flags)
```

## Detailed Description
AlignedAllocRealloc provides the ability to resize aligned memory allocations while maintaining the same alignment boundary as the original allocation. The function performs several key operations:

1. Extracts the original alignment value from the memory chunk metadata
2. Calculates the size of the original allocation (with some approximation due to context-specific rounding)
3. Allocates a new aligned chunk with the requested size in the same memory context
4. Copies the existing data from the old allocation to the new one
5. Frees the original unaligned allocation

The function handles memory context allocation failures gracefully and includes Valgrind memory debugging support. Due to the way memory contexts work (especially with power-of-2 rounding in aset contexts), the function may copy slightly more data than originally requested, but this is safe and only results in minor inefficiency.

## Parameters / Member Variables
- `pointer`: A pointer to aligned memory that was previously allocated using aligned allocation functions
- `size`: The new requested size for the allocation
- `flags`: Memory allocation flags that control allocation behavior

## Dependencies
- Functions called/Symbols referenced:
  - PointerGetMemoryChunk
  - [MemoryChunkGetValue](../M/MemoryChunkGetValue.md)
  - [MemoryChunkGetBlock](../M/MemoryChunkGetBlock.md)
  - GetMemoryChunkSpace
  - PallocAlignedExtraBytes
  - GetMemoryChunkContext
  - [MemoryContextAllocAligned](../M/MemoryContextAllocAligned.md)
  - [MemoryContextAllocationFailure](../M/MemoryContextAllocationFailure.md)
  - VALGRIND_MAKE_MEM_DEFINED
  - VALGRIND_MAKE_MEM_NOACCESS
  - memcpy
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - BOGUS_MCTX (memory context operations)
  - Referenced in memutils_internal.h

## Notes and Other Information
- The function maintains the same alignment boundary as the original allocation
- Size calculation includes approximation due to context-specific memory rounding
- Always allocates a new chunk rather than attempting in-place reallocation for simplicity
- Includes comprehensive Valgrind support for memory debugging
- Handles out-of-memory conditions by calling MemoryContextAllocationFailure
- May copy more data than the original request size due to memory context rounding, but this is safe
- Part of PostgreSQL's aligned memory allocation subsystem located in src/backend/utils/mmgr/alignedalloc.c