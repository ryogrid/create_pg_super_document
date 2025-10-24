# AllocSetFree

## Location
[src/backend/utils/mmgr/aset.c:1062-1168](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/aset.c#L1062-L1168)

## Overview
AllocSetFree frees allocated memory by removing it from the AllocSet and either returning the chunk to the appropriate freelist for reuse or releasing an entire block for large allocations.

## Definition

```c
void
AllocSetFree(void *pointer)
```
## Detailed Description
AllocSetFree handles memory deallocation for the AllocSet memory context. It implements two distinct code paths depending on whether the chunk being freed is a regular chunk within a block or an external chunk (large allocation that gets its own dedicated block):

1. **External chunks (large allocations)**: These are single-chunk blocks created for large allocations that exceed the allocChunkLimit. The function:
   - Validates the block structure and metadata
   - Removes the block from the block list by updating prev/next pointers
   - Updates the memory accounting
   - Optionally clobbers the freed memory for debugging
   - Calls free() to return the entire block to the system

2. **Regular chunks**: These are chunks allocated from multi-chunk blocks. The function:
   - Validates the chunk and its containing block
   - Determines the appropriate freelist based on the chunk's size class
   - Adds the chunk to the front of the corresponding freelist for future reuse
   - Marks the chunk as free in debugging builds
   - Optionally clobbers the freed memory for debugging

The function includes extensive error checking and debugging support, including detection of writes past the end of allocated chunks (buffer overruns) and optional memory clobbering to help catch use-after-free bugs.

## Parameters / Member Variables
- `*pointer`: Pointer to the memory chunk to be freed (as returned by AllocSetAlloc)
## Dependencies
- Functions called/Symbols referenced:
  - PointerGetMemoryChunk
  - [MemoryChunkIsExternal](../M/MemoryChunkIsExternal.md)
  - ExternalChunkGetBlock
  - AllocBlockIsValid
  - [MemoryChunkGetBlock](../M/MemoryChunkGetBlock.md)
  - [MemoryChunkGetValue](../M/MemoryChunkGetValue.md)
  - FreeListIdxIsValid
  - GetFreeListLink
  - GetChunkSizeFromFreeListIdx
  - [sentinel_ok](../s/sentinel_ok.md) (when MEMORY_CONTEXT_CHECKING enabled)
  - [wipe_mem](../w/wipe_mem.md) (when CLOBBER_FREED_MEMORY enabled)
  - free
  - elog
  - VALGRIND_MAKE_MEM_DEFINED
  - VALGRIND_MAKE_MEM_NOACCESS
- Called from:
  - [AllocSetRealloc](AllocSetRealloc.md)
  - BOGUS_MCTX (via function pointer)
  - Various components via the MemoryContext interface

## Notes and Other Information
- The function automatically determines whether a chunk is external or regular by checking the chunk header
- For external chunks, the entire block is immediately returned to the system via free()
- For regular chunks, memory is retained within the context for potential reuse through the freelist mechanism
- Includes buffer overrun detection using sentinel bytes when MEMORY_CONTEXT_CHECKING is enabled
- Supports optional memory clobbering with CLOBBER_FREED_MEMORY to help catch use-after-free bugs
- Uses different validation strategies for external vs regular chunks (runtime checks vs assertions)
- Updates memory accounting to track the context's total allocated memory
- Maintains Valgrind memory access tracking to help detect invalid memory access
- The freelist is managed as a simple LIFO (stack) structure for performance

## Simplified Source

```c
void AllocSetFree(void *pointer) {
    AllocSet set;
    MemoryChunk *chunk = PointerGetMemoryChunk(pointer);

    // Make chunk header accessible for memory validation tools
    VALGRIND_MAKE_MEM_DEFINED(chunk, ALLOC_CHUNKHDRSZ);

    if (MemoryChunkIsExternal(chunk)) {
        // Handle large allocations (single-chunk blocks)
        AllocBlock block = ExternalChunkGetBlock(chunk);

        // Validate block structure
        if (!AllocBlockIsValid(block) || block->freeptr != block->endptr)
            elog(ERROR, "could not find block containing chunk %p", chunk);

        set = block->aset;

        // Optional: Check for buffer overruns in debug builds
        #ifdef MEMORY_CONTEXT_CHECKING
        if (!sentinel_ok(pointer, chunk->requested_size))
            elog(WARNING, "detected write past chunk end in %s %p",
                 set->header.name, chunk);
        #endif

        // Remove block from linked list
        if (block->prev)
            block->prev->next = block->next;
        else
            set->blocks = block->next;
        if (block->next)
            block->next->prev = block->prev;

        // Update memory accounting and free the block
        set->header.mem_allocated -= block->endptr - ((char *) block);

        #ifdef CLOBBER_FREED_MEMORY
        wipe_mem(block, block->freeptr - ((char *) block));
        #endif
        free(block);
    }
    else {
        // Handle regular chunks from multi-chunk blocks
        AllocBlock block = MemoryChunkGetBlock(chunk);
        int fidx;
        AllocFreeListLink *link;

        // Validate block and get freelist index
        Assert(AllocBlockIsValid(block));
        set = block->aset;
        fidx = MemoryChunkGetValue(chunk);
        Assert(FreeListIdxIsValid(fidx));
        link = GetFreeListLink(chunk);

        // Optional: Check for buffer overruns and clobber memory
        #ifdef MEMORY_CONTEXT_CHECKING
        if (chunk->requested_size < GetChunkSizeFromFreeListIdx(fidx))
            if (!sentinel_ok(pointer, chunk->requested_size))
                elog(WARNING, "detected write past chunk end in %s %p",
                     set->header.name, chunk);
        #endif

        #ifdef CLOBBER_FREED_MEMORY
        wipe_mem(pointer, GetChunkSizeFromFreeListIdx(fidx));
        #endif

        // Add chunk to front of appropriate freelist
        VALGRIND_MAKE_MEM_DEFINED(link, sizeof(AllocFreeListLink));
        link->next = set->freelist[fidx];
        VALGRIND_MAKE_MEM_NOACCESS(link, sizeof(AllocFreeListLink));
        set->freelist[fidx] = chunk;

        #ifdef MEMORY_CONTEXT_CHECKING
        chunk->requested_size = InvalidAllocSize;
        #endif
    }
}
```