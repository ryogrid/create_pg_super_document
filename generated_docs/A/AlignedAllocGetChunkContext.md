# AlignedAllocGetChunkContext

## Location
[src/backend/utils/mmgr/alignedalloc.c:136-157](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/alignedalloc.c#L136-L157)

## Overview
Returns the MemoryContext that owns the given aligned allocation pointer, allowing memory context operations on aligned allocations.

## Definition
```c
MemoryContext AlignedAllocGetChunkContext(void *pointer)
```

## Detailed Description
AlignedAllocGetChunkContext provides a way to determine which memory context owns a specific aligned allocation. The function works by:

1. Retrieving the MemoryChunk metadata associated with the aligned pointer
2. Validating that the chunk is not an external chunk (which would be invalid for aligned allocations)
3. Extracting the original unaligned pointer from the chunk metadata
4. Getting the memory context from the original unaligned allocation
5. Properly managing Valgrind memory access states during the operation

This function is essential for memory context management operations that need to work with aligned allocations, enabling the memory context system to properly track and manage aligned memory chunks.

## Parameters / Member Variables
- `pointer`: A pointer to aligned memory that was previously allocated using aligned allocation functions

## Dependencies
- Functions called/Symbols referenced:
  - PointerGetMemoryChunk
  - [MemoryChunkIsExternal](../M/MemoryChunkIsExternal.md)
  - [MemoryChunkGetBlock](../M/MemoryChunkGetBlock.md)
  - [GetMemoryChunkContext](../G/GetMemoryChunkContext.md)
  - VALGRIND_MAKE_MEM_DEFINED
  - VALGRIND_MAKE_MEM_NOACCESS
- Called from (representative examples):
  - BOGUS_MCTX (memory context operations)
  - Referenced in memutils_internal.h

## Notes and Other Information
- The function includes proper Valgrind memory access management, temporarily making chunk metadata accessible for reading
- Assertions ensure that only non-external chunks are processed
- The function returns the context of the underlying unaligned allocation, not a separate context for aligned allocations
- Part of PostgreSQL's aligned memory allocation subsystem located in src/backend/utils/mmgr/alignedalloc.c
- Essential for integrating aligned allocations with PostgreSQL's memory context system

## Simplified Source

```c
MemoryContext AlignedAllocGetChunkContext(void *pointer) {
    MemoryChunk *redirchunk = PointerGetMemoryChunk(pointer);
    MemoryContext cxt;

    // Make chunk metadata accessible for reading
    VALGRIND_MAKE_MEM_DEFINED(redirchunk, sizeof(MemoryChunk));
    Assert(!MemoryChunkIsExternal(redirchunk));

    // Get context from the original unaligned allocation
    cxt = GetMemoryChunkContext(MemoryChunkGetBlock(redirchunk));

    // Restore memory access protection
    VALGRIND_MAKE_MEM_NOACCESS(redirchunk, sizeof(MemoryChunk));

    return cxt;
}
```