# SlabContextCreate

## Location
[src/backend/utils/mmgr/slab.c:322-430](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/slab.c#L322-L430)

## Overview
SlabContextCreate is the main constructor function that creates and initializes a new slab memory allocator context with specified block and chunk sizes.

## Definition
```c
MemoryContext SlabContextCreate(MemoryContext parent, const char *name, Size blockSize, Size chunkSize)
```

## Detailed Description
This function creates a specialized memory context optimized for allocating many fixed-size objects. It performs extensive validation of size parameters, calculates the optimal layout for chunks within blocks, and initializes all necessary data structures. The function computes how many chunks fit per block, determines the appropriate blocklist shift value for efficient indexing, and sets up the block management infrastructure including empty block tracking and blocklist arrays. It includes comprehensive error handling for out-of-memory conditions and parameter validation to ensure the slab allocator will function correctly.

The function calculates a blocklist_shift value that maps the number of free chunks in a block to an appropriate blocklist index efficiently using bit operations rather than division. It also handles memory context checking features when enabled and ensures proper alignment of all memory structures.

## Parameters / Member Variables
- `parent`: Parent memory context, or NULL if this is a top-level context
- `name`: Name of the context (must be statically allocated string)
- `blockSize`: Size in bytes of each memory block allocated from the system
- `chunkSize`: Size in bytes of each individual chunk allocated to users

## Dependencies
- Functions called/Symbols referenced:
  - [SlabContext](SlabContext.md) (struct type)
  - StaticAssertDecl (macro for compile-time assertions)
  - Slab_CHUNKHDRSZ, Slab_BLOCKHDRSZ, Slab_CONTEXT_HDRSZ (size constants)
  - MEMORYCHUNK_MAX_BLOCKOFFSET, MEMORYCHUNK_MAX_VALUE (limit constants)
  - MEMORY_CONTEXT_CHECKING (conditional compilation macro)
  - malloc (system memory allocation)
  - [MemoryContextStats](../M/MemoryContextStats.md), MemoryContextCreate (memory context framework functions)
  - [dclist_init](../d/dclist_init.md), dlist_init (doubly-linked list initialization)
  - SLAB_BLOCKLIST_COUNT (constant for number of blocklists)
  - MCTX_SLAB_ID (memory context type identifier)
- Called from (representative examples):
  - [ReorderBufferAllocate](../R/ReorderBufferAllocate.md)
  - RT_CREATE (radix tree creation)

## Notes and Other Information
- Returns a MemoryContext that can be cast to SlabContext internally
- Validates that chunk size constraints are met and block size limits are respected
- Automatically adjusts chunkSize to minimum of sizeof(MemoryChunk*) if smaller
- Calculates chunksPerBlock based on available space after block header
- Computes blocklist_shift for O(1) index calculations in block management
- Includes memory context checking infrastructure when enabled for debugging
- Uses malloc directly for context allocation to avoid bootstrapping issues
- Comprehensive error reporting with context name for debugging failed allocations