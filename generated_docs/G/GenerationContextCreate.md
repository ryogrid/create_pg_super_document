# GenerationContextCreate

## Location
src/backend/utils/mmgr/generation.c: 160 - 282

## Overview
Creates a new Generation memory context, which is a specialized memory management context that organizes memory into contiguous blocks for efficient allocation and deallocation patterns.

## Definition


## Detailed Description
GenerationContextCreate initializes a new Generation memory context, which is optimized for workloads that allocate many objects of similar sizes and then free them all at once. The context manages memory in blocks, starting with an initial block that contains the context header itself. The function validates allocation parameters, allocates the initial block using malloc, initializes the block structure, and sets up the context-specific parameters like chunk size limits.

The Generation context maintains a doubly-linked list of blocks and tracks the current allocation block. It calculates an allocation chunk limit based on the maximum block size to ensure efficient memory usage. The context is designed to handle both small chunks (allocated from blocks) and large allocations (handled separately).

## Parameters / Member Variables
- : Parent memory context, or NULL if this is a top-level context
- : Name of the context (must be statically allocated for the lifetime of the context)
- : Minimum size for the context's first block, or 0 to use initBlockSize
- : Initial size for allocation blocks (must be MAXALIGN'd and >= 1024 bytes)
- : Maximum size that blocks can grow to (must be MAXALIGN'd and <= MEMORYCHUNK_MAX_BLOCKOFFSET)

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