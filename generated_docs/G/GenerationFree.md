# GenerationFree

## Location
[src/backend/utils/mmgr/generation.c:689-799](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/generation.c#L689-L799)

## Overview
GenerationFree is the primary function for deallocating memory chunks in the Generation memory allocator, handling chunk deallocation and block management with intelligent block retention strategies.

## Definition
```c
void GenerationFree(void *pointer)
```

## Detailed Description
This function manages the deallocation of memory chunks within the Generation memory allocator system. It implements a sophisticated approach to memory management that goes beyond simple deallocation:

1. **Chunk Validation**: Determines whether the chunk is external or internal and validates the containing block
2. **Memory Safety**: Performs extensive safety checks including sentinel validation and memory corruption detection
3. **Reference Counting**: Updates the block's free chunk counter and tracks allocation status
4. **Block Management**: Implements intelligent decisions about when to retain or free empty blocks based on three key scenarios:
   - Keeper blocks: Never freed (part of context allocation)
   - Current blocks: Marked empty but retained for reuse
   - Free block optimization: Retains one empty block to avoid malloc/free cycles in FIFO workloads
5. **Debug Support**: Provides comprehensive debugging features including memory wiping and corruption detection

The function is designed to optimize memory usage patterns commonly found in PostgreSQL, particularly FIFO (First-In-First-Out) workloads where keeping one free block significantly reduces allocation overhead.

## Parameters / Member Variables
- `pointer`: Pointer to the memory chunk to be freed

## Dependencies
- Functions called/Symbols referenced:
  - PointerGetMemoryChunk - converts pointer to chunk header
  - [MemoryChunkIsExternal](../M/MemoryChunkIsExternal.md) - checks if chunk is externally allocated
  - ExternalChunkGetBlock / MemoryChunkGetBlock - retrieves containing block
  - GenerationBlockIsValid - validates block integrity
  - [MemoryChunkGetValue](../M/MemoryChunkGetValue.md) - gets chunk size information
  - [sentinel_ok](../s/sentinel_ok.md) - validates memory bounds (debug builds)
  - [wipe_mem](../w/wipe_mem.md) - clears freed memory (debug builds)
  - IsKeeperBlock - checks if block is the keeper block
  - [GenerationBlockMarkEmpty](GenerationBlockMarkEmpty.md) - marks block as empty for reuse
  - [GenerationBlockFree](GenerationBlockFree.md) - actually frees the block memory
- Called from:
  - [GenerationRealloc](GenerationRealloc.md) - during memory reallocation operations
  - BOGUS_MCTX - as part of memory context method table
  - General memory context operations throughout PostgreSQL

## Notes and Other Information
- Implements a three-tier strategy for empty block management to optimize common PostgreSQL memory usage patterns
- Extensive debugging support with compile-time options for memory checking and corruption detection
- Uses Valgrind integration for memory debugging in development builds
- The "freeblock" optimization is specifically designed for FIFO workloads to prevent continuous malloc/free cycles
- Block validation differs between external and internal chunks for performance reasons
- Memory corruption detection through sentinel values helps identify buffer overruns in debug builds
- The function never actually frees keeper blocks, which are allocated as part of the context structure

## Simplified Source

```c
void
GenerationFree(void *pointer)
{
    MemoryChunk *chunk = PointerGetMemoryChunk(pointer);
    GenerationBlock *block;
    GenerationContext *set;

    // Determine if chunk is external or internal and get block
    if (MemoryChunkIsExternal(chunk))
    {
        block = ExternalChunkGetBlock(chunk);
        if (!GenerationBlockIsValid(block))
            elog(ERROR, "could not find block containing chunk %p", chunk);
    }
    else
    {
        block = MemoryChunkGetBlock(chunk);
        Assert(GenerationBlockIsValid(block));
    }

    // Perform memory safety checks (if debugging enabled)
    #ifdef MEMORY_CONTEXT_CHECKING
        // Check for corruption and reset chunk metadata
        if (!sentinel_ok(pointer, chunk->requested_size))
            elog(WARNING, "detected write past chunk end in %s %p",
                 ((MemoryContext) block->context)->name, chunk);
        chunk->requested_size = InvalidAllocSize;
    #endif

    // Update block's free chunk counter
    block->nfree += 1;

    // If block still has allocated chunks, we're done
    if (likely(block->nfree < block->nchunks))
        return;

    // Block is now empty - decide what to do with it
    set = block->context;

    if (IsKeeperBlock(set, block) || set->block == block)
        // Case 1 & 2: Keeper or current block - just mark empty
        GenerationBlockMarkEmpty(block);
    else if (set->freeblock == NULL)
    {
        // Case 3: Keep as freeblock to avoid malloc/free cycles
        GenerationBlockMarkEmpty(block);
        set->freeblock = block;
    }
    else
        // Otherwise, actually free the block
        GenerationBlockFree(set, block);
}
```