# AlignedAllocGetChunkSpace

## Location
[src/backend/utils/mmgr/alignedalloc.c:158-172](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/alignedalloc.c#L158-L172)

## Overview
Returns the total memory space occupied by an aligned allocation, including all memory allocation overhead from the underlying unaligned allocation.

## Definition
```c
Size AlignedAllocGetChunkSpace(void *pointer)
```

## Detailed Description
AlignedAllocGetChunkSpace provides the ability to determine the total memory footprint of an aligned allocation. The function works by:

1. Retrieving the MemoryChunk metadata associated with the aligned pointer
2. Extracting the original unaligned pointer from the chunk metadata
3. Calling GetMemoryChunkSpace() on the underlying unaligned allocation to get the total space
4. Returning the space value, which includes all memory allocation overhead

The returned value represents the total space used by the underlying unaligned allocation, which includes the actual data, alignment padding, memory context overhead, and any additional space allocated by the memory context (such as power-of-2 rounding in aset contexts). This function is useful for memory usage analysis and debugging.

## Parameters / Member Variables
- `pointer`: A pointer to aligned memory that was previously allocated using aligned allocation functions

## Dependencies
- Functions called/Symbols referenced:
  - PointerGetMemoryChunk
  - [MemoryChunkGetBlock](../M/MemoryChunkGetBlock.md)
  - [GetMemoryChunkSpace](../G/GetMemoryChunkSpace.md)
  - VALGRIND_MAKE_MEM_DEFINED
  - VALGRIND_MAKE_MEM_NOACCESS
- Called from (representative examples):
  - BOGUS_MCTX (memory context operations)
  - Referenced in memutils_internal.h

## Notes and Other Information
- The function returns the space of the underlying unaligned allocation, not just the aligned portion
- Includes proper Valgrind memory access management for debugging support
- The returned size may be larger than the originally requested size due to memory context allocation policies
- Essential for memory usage tracking and analysis of aligned allocations
- Part of PostgreSQL's aligned memory allocation subsystem located in src/backend/utils/mmgr/alignedalloc.c
- Works in conjunction with PostgreSQL's memory context system to provide accurate space reporting