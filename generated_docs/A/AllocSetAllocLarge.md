# AllocSetAllocLarge

## Location
[src/backend/utils/mmgr/aset.c:696-773](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/aset.c#L696-L773)

## Overview
Allocates large memory chunks that require their own dedicated block, implementing PostgreSQL's allocation strategy for requests exceeding the standard chunk size limits.

## Definition

```c
static void *
AllocSetAllocLarge(MemoryContext context, Size size, int flags)
```
## Detailed Description
AllocSetAllocLarge is a specialized helper function for AllocSetAlloc() that handles large memory allocations requiring dedicated blocks. When a requested allocation size exceeds the context's allocChunkLimit, this function creates an entire new block exclusively for that single chunk.

The function implements several key features:
- Creates a block sized exactly for the requested chunk plus necessary headers
- Marks chunks as "externally managed" to indicate they occupy entire blocks
- Integrates new blocks into the context's block list without disrupting active allocation
- Supports comprehensive memory debugging through sentinels, randomization, and Valgrind integration
- Handles allocation failure scenarios through standardized error reporting

The block insertion strategy places new large blocks as the second block in the chain (after the current active block), preserving remaining space in active blocks for smaller allocations while maintaining efficient block list management.

## Parameters / Member Variables
- `context`: The AllocSet memory context for the allocation (cast internally to AllocSet)
- `size`: Size in bytes of the requested allocation
- `flags`: Allocation flags controlling behavior and error handling
## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextCheckSize (validates size against context limits and flags)
  - [MemoryContextAllocationFailure](../M/MemoryContextAllocationFailure.md) (handles allocation failure scenarios)
  - malloc (system memory allocation)
  - [MemoryChunkSetHdrMaskExternal](../M/MemoryChunkSetHdrMaskExternal.md) (marks chunk as externally managed)
  - MemoryChunkGetPointer (retrieves user-accessible pointer from chunk)
  - [set_sentinel](../s/set_sentinel.md) (adds debugging sentinel bytes)
  - [randomize_mem](../r/randomize_mem.md) (fills memory with random data for debugging)
  - VALGRIND_MAKE_MEM_NOACCESS (memory debugging support)

- Referenced constants/macros:
  - ALLOC_BLOCKHDRSZ, ALLOC_CHUNKHDRSZ (header sizes)
  - MCTX_ASET_ID (AllocSet context type identifier)
  - MAXALIGN (memory alignment macro)
  - MEMORY_CONTEXT_CHECKING, RANDOMIZE_ALLOCATED_MEMORY (debug options)

- Called from (representative examples):
  - [AllocSetAlloc](AllocSetAlloc.md) (when allocation size exceeds chunk limits)

## Notes and Other Information
- Function marked as pg_noinline to keep AllocSetAlloc() optimized for common small allocation cases
- Large allocations get dedicated blocks to avoid fragmentation in regular allocation blocks
- Block insertion strategy preserves active block space by inserting new blocks as second in chain
- Externally managed chunks are handled differently during free operations since they occupy entire blocks
- Memory debugging features include sentinel bytes, memory randomization, and Valgrind integration
- Chunk headers are marked inaccessible to prevent accidental corruption
- Padding bytes beyond requested size are marked inaccessible for memory safety
- Handles allocation failures through context-specific failure reporting mechanisms
- Block size calculation includes space for block header, chunk header, and proper alignment
- The allocation strategy balances memory efficiency with allocation performance for large requests

## Simplified Source

```c
static void *
AllocSetAllocLarge(MemoryContext context, Size size, int flags)
{
    AllocSet set = (AllocSet) context;
    AllocBlock block;
    MemoryChunk *chunk;
    Size chunk_size, blksize;

    // Validate size for the given flags
    MemoryContextCheckSize(context, size, flags);

    // Calculate chunk size (aligned, with optional debug padding)
#ifdef MEMORY_CONTEXT_CHECKING
    chunk_size = MAXALIGN(size + 1);  // Extra byte for sentinel
#else
    chunk_size = MAXALIGN(size);
#endif

    // Allocate dedicated block for this large chunk
    blksize = chunk_size + ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ;
    block = (AllocBlock) malloc(blksize);
    if (block == NULL) {
        return MemoryContextAllocationFailure(context, size, flags);
    }

    context->mem_allocated += blksize;

    // Initialize block structure
    block->aset = set;
    block->freeptr = block->endptr = ((char *) block) + blksize;

    // Setup chunk as externally managed (occupies entire block)
    chunk = (MemoryChunk *) (((char *) block) + ALLOC_BLOCKHDRSZ);
    MemoryChunkSetHdrMaskExternal(chunk, MCTX_ASET_ID);

#ifdef MEMORY_CONTEXT_CHECKING
    chunk->requested_size = size;
    if (size < chunk_size) {
        set_sentinel(MemoryChunkGetPointer(chunk), size);
    }
#endif

    // Insert block into block list (as second block to preserve active space)
    if (set->blocks != NULL) {
        block->prev = set->blocks;
        block->next = set->blocks->next;
        if (block->next) {
            block->next->prev = block;
        }
        set->blocks->next = block;
    } else {
        block->prev = NULL;
        block->next = NULL;
        set->blocks = block;
    }

    return MemoryChunkGetPointer(chunk);
}
```