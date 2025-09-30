# BumpContextCreate

## Location
[src/backend/utils/mmgr/bump.c:131-242](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/bump.c#L131-L242)

## Overview
Creates a new Bump memory context, which is a specialized memory allocation context optimized for append-only allocation patterns with efficient bulk deallocation.

## Definition

```c
MemoryContext
BumpContextCreate(MemoryContext parent, const char *name, Size minContextSize,
				  Size initBlockSize, Size maxBlockSize)
```
## Detailed Description
BumpContextCreate initializes a Bump memory context that provides efficient memory allocation for scenarios where memory is primarily allocated sequentially and freed all at once. The context uses a block-based allocation strategy where memory is allocated from contiguous blocks, and when a block is exhausted, a new larger block is allocated. This design is particularly efficient for temporary data structures that grow incrementally and are discarded entirely.

The function performs extensive validation of input parameters, allocates the initial block containing both the context header and block header, initializes the block management structures, and sets up allocation limits based on the maximum block size and chunk constraints.

## Parameters / Member Variables
- : Parent memory context, or NULL if this is a top-level context
- : Name of the context (must be statically allocated string)
- : Minimum size for the initial context allocation
- : Initial size for allocation blocks (must be ≥1024 and MAXALIGNED)
- : Maximum size for allocation blocks (must be ≥initBlockSize and ≤MEMORYCHUNK_MAX_BLOCKOFFSET)

## Dependencies
- Functions called/Symbols referenced:
  - malloc
  - [MemoryContextStats](../M/MemoryContextStats.md)
  - [MemoryContextCreate](../M/MemoryContextCreate.md)
  - [dlist_init](../d/dlist_init.md)
  - [dlist_push_head](../d/dlist_push_head.md)
  - KeeperBlock
  - [BumpBlockInit](BumpBlockInit.md)
  - StaticAssertDecl
  - AllocHugeSizeIsValid
- Called from (representative examples):
  - [TidStoreCreateLocal](../T/TidStoreCreateLocal.md)
  - [tuplesort_begin_batch](../t/tuplesort_begin_batch.md)

## Notes and Other Information
- The initial block layout is unique compared to other Bump blocks as it starts with the context header followed by the block header
- The function calculates allocChunkLimit to ensure efficient space utilization, limiting chunk sizes to fit at least Bump_CHUNK_FRACTION chunks per maximum block
- All size parameters must be MAXALIGNED and the function enforces minimum sizes and maximum limits for memory safety
- The context uses a doubly-linked list to manage blocks for efficient traversal during reset operations

## Simplified Source

```c
MemoryContext
BumpContextCreate(MemoryContext parent, const char *name, Size minContextSize,
                  Size initBlockSize, Size maxBlockSize)
{
    Size allocSize;
    BumpContext *set;
    BumpBlock *block;

    // Validate parameters (assertions simplified)
    Assert(initBlockSize >= 1024 && maxBlockSize >= initBlockSize);
    Assert(maxBlockSize <= MEMORYCHUNK_MAX_BLOCKOFFSET);

    // Calculate initial allocation size
    allocSize = MAXALIGN(sizeof(BumpContext)) + Bump_BLOCKHDRSZ + Bump_CHUNKHDRSZ;
    if (minContextSize != 0)
        allocSize = Max(allocSize, minContextSize);
    else
        allocSize = Max(allocSize, initBlockSize);

    // Allocate initial block
    set = (BumpContext *) malloc(allocSize);
    if (set == NULL) {
        // Error handling for out of memory
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
                        errmsg("out of memory")));
    }

    // Initialize block management
    dlist_init(&set->blocks);
    block = KeeperBlock(set);
    Size firstBlockSize = allocSize - MAXALIGN(sizeof(BumpContext));
    BumpBlockInit(set, block, firstBlockSize);
    dlist_push_head(&set->blocks, &block->node);

    // Set context configuration
    set->initBlockSize = (uint32) initBlockSize;
    set->maxBlockSize = (uint32) maxBlockSize;
    set->nextBlockSize = (uint32) initBlockSize;

    // Calculate allocation chunk limit
    set->allocChunkLimit = Min(maxBlockSize, MEMORYCHUNK_MAX_VALUE);
    while ((Size)(set->allocChunkLimit + Bump_CHUNKHDRSZ) >
           (Size)((maxBlockSize - Bump_BLOCKHDRSZ) / Bump_CHUNK_FRACTION))
        set->allocChunkLimit >>= 1;

    // Create the memory context
    MemoryContextCreate((MemoryContext) set, T_BumpContext, MCTX_BUMP_ID,
                        parent, name);
    ((MemoryContext) set)->mem_allocated = allocSize;

    return (MemoryContext) set;
}
```