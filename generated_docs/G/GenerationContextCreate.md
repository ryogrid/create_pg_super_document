# GenerationContextCreate

## Location
[src/backend/utils/mmgr/generation.c:160-282](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/generation.c#L160-L282)

## Overview
Creates a new Generation memory context, which is a specialized memory management context that organizes memory into contiguous blocks for efficient allocation and deallocation patterns.

## Definition

```c
MemoryContext
GenerationContextCreate(MemoryContext parent,
						const char *name,
						Size minContextSize,
						Size initBlockSize,
						Size maxBlockSize)
```
## Detailed Description
GenerationContextCreate initializes a new Generation memory context, which is optimized for workloads that allocate many objects of similar sizes and then free them all at once. The context manages memory in blocks, starting with an initial block that contains the context header itself. The function validates allocation parameters, allocates the initial block using malloc, initializes the block structure, and sets up the context-specific parameters like chunk size limits.

The Generation context maintains a doubly-linked list of blocks and tracks the current allocation block. It calculates an allocation chunk limit based on the maximum block size to ensure efficient memory usage. The context is designed to handle both small chunks (allocated from blocks) and large allocations (handled separately).

## Parameters / Member Variables
- `parent`: Parent memory context, or NULL if this is a top-level context
- `*name`: Name of the context (must be statically allocated for the lifetime of the context)
- `minContextSize`: Minimum size for the context's first block, or 0 to use initBlockSize
- `initBlockSize`: Initial size for allocation blocks (must be MAXALIGN'd and >= 1024 bytes)
- `maxBlockSize`: Maximum size that blocks can grow to (must be MAXALIGN'd and <= MEMORYCHUNK_MAX_BLOCKOFFSET)
## Dependencies
- Functions called/Symbols referenced:
  - malloc
  - [MemoryContextStats](../M/MemoryContextStats.md)
  - [MemoryContextCreate](../M/MemoryContextCreate.md)
  - [dlist_init](../d/dlist_init.md)
  - [dlist_push_head](../d/dlist_push_head.md)
  - KeeperBlock
  - [GenerationBlockInit](GenerationBlockInit.md)
  - StaticAssertDecl
  - AllocHugeSizeIsValid
- Called from (representative examples):
  - [gistvacuumscan](../g/gistvacuumscan.md)
  - [ReorderBufferAllocate](../R/ReorderBufferAllocate.md)

## Notes and Other Information
- The function enforces strict validation of block size parameters with assertions
- The initial block is special as it contains both the GenerationContext header and a GenerationBlock
- Block sizes must be properly aligned (MAXALIGN) and within specified limits
- The allocChunkLimit is calculated to ensure at least Generation_CHUNK_FRACTION chunks can fit in a maximum-sized block
- Memory allocation failure triggers an ERROR with detailed context information
- The context uses a doubly-linked list to track all blocks for efficient management

## Simplified Source

```c
MemoryContext
GenerationContextCreate(MemoryContext parent,
                       const char *name,
                       Size minContextSize,
                       Size initBlockSize,
                       Size maxBlockSize)
{
    GenerationContext *context;
    GenerationBlock *initial_block;
    Size allocSize;

    // Validate parameters - ensure sizes are aligned and within limits
    Assert(initBlockSize >= 1024 && initBlockSize == MAXALIGN(initBlockSize));
    Assert(maxBlockSize >= initBlockSize && maxBlockSize <= MEMORYCHUNK_MAX_BLOCKOFFSET);

    // Calculate size needed for context + initial block
    allocSize = MAXALIGN(sizeof(GenerationContext)) +
                Generation_BLOCKHDRSZ + Generation_CHUNKHDRSZ;
    allocSize = Max(allocSize, minContextSize ? minContextSize : initBlockSize);

    // Allocate memory for the context and initial block
    context = (GenerationContext *) malloc(allocSize);
    if (context == NULL) {
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
                       errmsg("out of memory")));
    }

    // Initialize block list and set up initial block
    dlist_init(&context->blocks);
    initial_block = KeeperBlock(context);
    GenerationBlockInit(context, initial_block,
                       allocSize - MAXALIGN(sizeof(GenerationContext)));
    dlist_push_head(&context->blocks, &initial_block->node);

    // Set context parameters
    context->block = initial_block;
    context->freeblock = NULL;
    context->initBlockSize = initBlockSize;
    context->maxBlockSize = maxBlockSize;
    context->nextBlockSize = initBlockSize;

    // Calculate chunk size limit for efficient allocation
    context->allocChunkLimit = Min(maxBlockSize, MEMORYCHUNK_MAX_VALUE);
    while ((context->allocChunkLimit + Generation_CHUNKHDRSZ) >
           ((maxBlockSize - Generation_BLOCKHDRSZ) / Generation_CHUNK_FRACTION)) {
        context->allocChunkLimit >>= 1;
    }

    // Complete context creation
    MemoryContextCreate((MemoryContext) context, T_GenerationContext,
                       MCTX_GENERATION_ID, parent, name);

    return (MemoryContext) context;
}
```