# AllocSetContextCreateInternal

## Location
[src/backend/utils/mmgr/aset.c:347-536](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/aset.c#L347-L536)

## Overview
Creates a new AllocSet memory context with specified size parameters, implementing PostgreSQL's primary memory management mechanism with optimized block allocation and freelist management.

## Definition

```c
MemoryContext
AllocSetContextCreateInternal(MemoryContext parent,
							  const char *name,
							  Size minContextSize,
							  Size initBlockSize,
							  Size maxBlockSize)
```
## Detailed Description
AllocSetContextCreateInternal is the core function for creating PostgreSQL's AllocSet memory contexts. It implements sophisticated memory management by establishing freelists for different allocation sizes, managing memory blocks efficiently, and providing context recycling for improved performance.

The function first attempts to reuse an existing context from freelists (context_freelists[]) if the parameters match standard configurations (default or small). If no suitable context exists for reuse, it allocates a new context structure with an initial memory block.

Key design aspects include:
- Context recycling through freelists for common size configurations
- Initial block sizing based on context requirements vs. requested block size
- Chunk size limit calculation to optimize memory usage and minimize waste
- Integration with PostgreSQL's memory context hierarchy
- Comprehensive parameter validation and alignment checking

## Parameters / Member Variables
- `parent`: Parent memory context in the hierarchy, or NULL for top-level contexts
- `*name`: Context name (must be statically allocated string for identification/debugging)
- `minContextSize`: Minimum size for the context's initial block (0 = use initBlockSize)
- `initBlockSize`: Initial allocation block size for the context
- `maxBlockSize`: Maximum size for any single allocation block
## Dependencies
- Functions called/Symbols referenced:
  - StaticAssertDecl (compile-time assertions for alignment)
  - AllocHugeSizeIsValid (validates block size limits)
  - malloc (system memory allocation)
  - [MemoryContextCreate](../M/MemoryContextCreate.md) (generic context initialization)
  - [MemoryContextStats](../M/MemoryContextStats.md) (memory usage reporting on errors)
  - KeeperBlock (macro to access initial block)
  - MemSetAligned (aligned memory initialization)
  - VALGRIND_MAKE_MEM_NOACCESS (memory debugging support)
  
- Referenced constants:
  - ALLOC_CHUNKHDRSZ, ALLOC_BLOCKHDRSZ (header sizes)
  - ALLOCSET_DEFAULT_MINSIZE/INITSIZE, ALLOCSET_SMALL_MINSIZE/INITSIZE
  - ALLOC_CHUNK_LIMIT, ALLOCSET_SEPARATE_THRESHOLD
  - MEMORYCHUNK_MAX_BLOCKOFFSET (addressing limit)

- Called from (representative examples):  
  - AllocSetContextCreate (public wrapper macro)
  - MemoryContextCopyAndSetIdentifier (context duplication)

## Notes and Other Information
- Context recycling optimization: Reuses freed contexts with matching parameters from global freelists
- Supports two standard size configurations (default and small) for common use cases
- Implements power-of-2 chunk size limits to optimize memory block utilization
- Uses keeper blocks where the context header and first data block are contiguous 
- Includes comprehensive compile-time and runtime assertions for parameter validation
- Integrates with Valgrind for memory debugging in development builds
- The allocChunkLimit calculation ensures ~1/8 maximum waste ratio for streaming allocations
- Failure to allocate initial block triggers detailed memory context statistics before error reporting

## Simplified Source

```c
MemoryContext
AllocSetContextCreateInternal(MemoryContext parent,
                              const char *name,
                              Size minContextSize,
                              Size initBlockSize,
                              Size maxBlockSize)
{
    AllocSet set;
    AllocBlock block;
    Size firstBlockSize;
    int freeListIndex = -1;

    // Validate parameters (initBlockSize >= 1K, properly aligned, etc.)
    Assert(initBlockSize >= 1024 && initBlockSize == MAXALIGN(initBlockSize));
    Assert(maxBlockSize >= initBlockSize && maxBlockSize <= MEMORYCHUNK_MAX_BLOCKOFFSET);

    // Check if we can reuse an existing context from freelists
    if (minContextSize == ALLOCSET_DEFAULT_MINSIZE &&
        initBlockSize == ALLOCSET_DEFAULT_INITSIZE) {
        freeListIndex = 0;
    } else if (minContextSize == ALLOCSET_SMALL_MINSIZE &&
               initBlockSize == ALLOCSET_SMALL_INITSIZE) {
        freeListIndex = 1;
    }

    // Try to recycle from freelist if possible
    if (freeListIndex >= 0 && context_freelists[freeListIndex].first_free != NULL) {
        set = context_freelists[freeListIndex].first_free;
        context_freelists[freeListIndex].first_free = (AllocSet) set->header.nextchild;
        context_freelists[freeListIndex].num_free--;

        // Update parameters and reinitialize
        set->maxBlockSize = maxBlockSize;
        MemoryContextCreate((MemoryContext) set, T_AllocSetContext,
                           MCTX_ASET_ID, parent, name);
        return (MemoryContext) set;
    }

    // Calculate size for initial block
    firstBlockSize = MAXALIGN(sizeof(AllocSetContext)) +
                     ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ;
    firstBlockSize = Max(firstBlockSize,
                        minContextSize ? minContextSize : initBlockSize);

    // Allocate the initial block with context header
    set = (AllocSet) malloc(firstBlockSize);
    if (set == NULL) {
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
                       errmsg("out of memory")));
    }

    // Initialize the keeper block
    block = KeeperBlock(set);
    block->aset = set;
    block->freeptr = ((char *) block) + ALLOC_BLOCKHDRSZ;
    block->endptr = ((char *) set) + firstBlockSize;
    block->prev = NULL;
    block->next = NULL;
    set->blocks = block;

    // Initialize context-specific fields
    MemSetAligned(set->freelist, 0, sizeof(set->freelist));
    set->initBlockSize = initBlockSize;
    set->maxBlockSize = maxBlockSize;
    set->nextBlockSize = initBlockSize;
    set->freeListIndex = freeListIndex;

    // Calculate chunk size limit (power-of-2, <= ALLOC_CHUNK_LIMIT)
    set->allocChunkLimit = ALLOC_CHUNK_LIMIT;
    while ((set->allocChunkLimit + ALLOC_CHUNKHDRSZ) >
           ((maxBlockSize - ALLOC_BLOCKHDRSZ) / ALLOC_CHUNK_FRACTION)) {
        set->allocChunkLimit >>= 1;
    }

    // Complete context creation
    MemoryContextCreate((MemoryContext) set, T_AllocSetContext,
                       MCTX_ASET_ID, parent, name);
    ((MemoryContext) set)->mem_allocated = firstBlockSize;

    return (MemoryContext) set;
}
```