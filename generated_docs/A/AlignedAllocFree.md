# AlignedAllocFree

## Location
[src/backend/utils/mmgr/alignedalloc.c:29-60](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/alignedalloc.c#L29-L60)

## Overview
Frees memory allocated by aligned allocation functions, properly handling the metadata and returning the memory to its owning memory context.

## Definition

```c
void
AlignedAllocFree(void *pointer)
```
## Detailed Description
AlignedAllocFree is responsible for freeing memory that was allocated using PostgreSQL's aligned allocation system. The function performs several important operations:

1. Retrieves the MemoryChunk metadata from the provided pointer
2. Validates that the chunk is not an external chunk
3. Recovers the original unaligned pointer that was initially allocated
4. Performs memory corruption checks if MEMORY_CONTEXT_CHECKING is enabled
5. Recursively frees the unaligned chunk using pfree()

The function is designed to work with PostgreSQL's memory context system, ensuring that aligned allocations are properly tracked and freed within their respective memory contexts.

## Parameters / Member Variables
- `*pointer`: A pointer to aligned memory that was previously allocated using aligned allocation functions
## Dependencies
- Functions called/Symbols referenced:
  - PointerGetMemoryChunk
  - [MemoryChunkIsExternal](../M/MemoryChunkIsExternal.md)
  - [MemoryChunkGetBlock](../M/MemoryChunkGetBlock.md)
  - VALGRIND_MAKE_MEM_DEFINED
  - [sentinel_ok](../s/sentinel_ok.md) (conditional, for memory checking)
  - [GetMemoryChunkContext](../G/GetMemoryChunkContext.md) (conditional, for memory checking)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - BOGUS_MCTX (memory context operations)
  - Referenced in memutils_internal.h

## Notes and Other Information
- The function includes Valgrind support for memory debugging
- Memory corruption detection is available when MEMORY_CONTEXT_CHECKING is enabled
- The function works by recovering the original unaligned pointer and then using the standard pfree() function
- Assertions ensure that external chunks are not processed by this function
- The function is part of PostgreSQL's aligned memory allocation subsystem located in src/backend/utils/mmgr/alignedalloc.c