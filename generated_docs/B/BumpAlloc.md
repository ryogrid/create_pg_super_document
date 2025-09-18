# BumpAlloc

## Location
[src/backend/utils/mmgr/bump.c:491-534](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/bump.c#L491-L534)

## Overview
BumpAlloc is the primary allocation function for the bump memory context, providing fast sequential memory allocation by incrementing a pointer within blocks.

## Definition


## Detailed Description
This function implements the main allocation logic for the bump memory allocator. It first calculates the required chunk size with proper alignment, then determines whether to handle the request as a regular chunk or delegate to BumpAllocLarge for oversized allocations. For regular allocations, it attempts to allocate from the current block, and if insufficient space is available, calls BumpAllocFromNewBlock to create a new block. The function is optimized for performance with the most common code paths inline and less common scenarios delegated to helper functions.

## Parameters / Member Variables
- : The memory context to allocate from (must be a BumpContext)
- : The number of bytes to allocate
- : Allocation flags controlling behavior (e.g., MCXT_ALLOC_NO_OOM for NULL on failure)

## Dependencies
- Functions called/Symbols referenced:
  - BumpContext (cast context to bump-specific type)
  - BumpBlock (memory block structure)
  - BumpIsValid (validates the context structure)
  - MAXALIGN (aligns chunk size to platform requirements)
  - MEMORY_CONTEXT_CHECKING (conditional compilation for debugging)
  - [BumpAllocLarge](BumpAllocLarge.md) (handles oversized allocations)
  - Bump_CHUNKHDRSZ (chunk header size constant)
  - [dlist_head_node](../d/dlist_head_node.md) (gets first node in block list)
  - dlist_container (converts node to containing block)
  - [BumpBlockFreeBytes](BumpBlockFreeBytes.md) (checks available space in block)
  - [BumpAllocFromNewBlock](BumpAllocFromNewBlock.md) (creates new block when needed)
  - [BumpAllocChunkFromBlock](BumpAllocChunkFromBlock.md) (allocates chunk from existing block)
- Called from (representative examples):
  - Memory allocation macros and functions throughout PostgreSQL
  - BOGUS_MCTX (test/debugging context)

## Notes and Other Information
- Optimized for performance with inline fast path and noinline helper functions
- Supports memory context checking mode with sentinel bytes
- Handles allocation failure according to flags (ERROR or NULL return)
- Maximum allocation size is MAXALIGN_DOWN(SIZE_MAX) - Bump_BLOCKHDRSZ - Bump_CHUNKHDRSZ
- Uses MAXALIGN to ensure proper memory alignment for all platforms
- Designed to minimize stack frame overhead for the common allocation path