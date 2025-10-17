# BumpAllocChunkFromBlock

## Location
[src/backend/utils/mmgr/bump.c:371-429](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/bump.c#L371-L429)

## Overview
A utility function that allocates a chunk from an existing block with sufficient free space, handling both memory context checking and non-checking builds.

## Definition

```c
static inline void *
BumpAllocChunkFromBlock(MemoryContext context, BumpBlock *block, Size size,
						Size chunk_size)
```
## Detailed Description
BumpAllocChunkFromBlock is a low-level allocation helper that performs the actual work of carving out a chunk from a block that is known to have sufficient free space. The function advances the block's freeptr by the required amount (chunk header plus aligned chunk size), initializes the chunk header in memory context checking builds, and returns a pointer to the usable memory area.

This function centralizes the common chunk allocation logic shared between BumpAlloc (for allocations from the current block) and BumpAllocFromNewBlock (for allocations from newly created blocks). It includes comprehensive memory debugging support with Valgrind integration, sentinel byte protection, and memory randomization when enabled.

## Parameters / Member Variables
- `context`: The Bump memory context (used for debugging/checking)
- `block`: The block to allocate the chunk from (must have sufficient free space)
- `size`: The requested allocation size in bytes
- `chunk_size`: The aligned chunk size to allocate (includes padding)

## Dependencies
- Functions called/Symbols referenced:
  - VALGRIND_MAKE_MEM_UNDEFINED (in MEMORY_CONTEXT_CHECKING builds)
  - [MemoryChunkSetHdrMask](../M/MemoryChunkSetHdrMask.md) (in MEMORY_CONTEXT_CHECKING builds)
  - [set_sentinel](../s/set_sentinel.md) (in MEMORY_CONTEXT_CHECKING builds)
  - MemoryChunkGetPointer (in MEMORY_CONTEXT_CHECKING builds)
  - [randomize_mem](../r/randomize_mem.md) (if RANDOMIZE_ALLOCATED_MEMORY defined)
  - VALGRIND_MAKE_MEM_NOACCESS (in MEMORY_CONTEXT_CHECKING builds)
- Called from (representative examples):
  - [BumpAllocFromNewBlock](BumpAllocFromNewBlock.md)
  - [BumpAlloc](BumpAlloc.md)

## Notes and Other Information
- The function is marked static inline for performance, as it's called frequently during allocation
- Includes assertions to validate that the block has sufficient space before allocation
- The implementation varies significantly between memory context checking and non-checking builds
- In checking builds, chunk headers are properly initialized with metadata for debugging and validation
- Sentinel bytes are placed after the requested size to detect buffer overruns
- Valgrind integration marks unused padding bytes as inaccessible to detect memory access errors
- The function assumes the caller has already validated size parameters and block availability

## Simplified Source

```c
static inline void *
BumpAllocChunkFromBlock(MemoryContext context, BumpBlock *block, Size size,
                        Size chunk_size)
{
#ifdef MEMORY_CONTEXT_CHECKING
    MemoryChunk *chunk;
#else
    void *ptr;
#endif

    // Validate sufficient space is available
    Assert(block != NULL);
    Assert((block->endptr - block->freeptr) >= Bump_CHUNKHDRSZ + chunk_size);

#ifdef MEMORY_CONTEXT_CHECKING
    chunk = (MemoryChunk *) block->freeptr;
#else
    ptr = (void *) block->freeptr;
#endif

    // Advance freeptr beyond this chunk
    block->freeptr += (Bump_CHUNKHDRSZ + chunk_size);
    Assert(block->freeptr <= block->endptr);

#ifdef MEMORY_CONTEXT_CHECKING
    // Initialize chunk header for memory debugging
    MemoryChunkSetHdrMask(chunk, block, chunk_size, MCTX_BUMP_ID);
    chunk->requested_size = size;

    // Add sentinel for buffer overrun detection
    set_sentinel(MemoryChunkGetPointer(chunk), size);

#ifdef RANDOMIZE_ALLOCATED_MEMORY
    // Fill allocated space with debugging pattern
    randomize_mem((char *) MemoryChunkGetPointer(chunk), size);
#endif

    return MemoryChunkGetPointer(chunk);
#else
    return ptr;
#endif
}
```