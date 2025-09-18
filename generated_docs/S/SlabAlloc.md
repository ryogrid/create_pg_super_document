# SlabAlloc

## Location
src/backend/utils/mmgr/slab.c: 630 - 700

## Overview
SlabAlloc is the main allocation function for the slab memory allocator that returns a pointer to a newly allocated memory chunk of a fixed size.

## Definition


## Detailed Description
SlabAlloc is the primary allocation function for PostgreSQL's slab memory allocator, designed for high-performance allocation of fixed-size memory chunks. The function implements a fast-path optimization strategy: it handles the most common allocation scenarios in the main function body to avoid stack frame overhead, while delegating edge cases to helper functions. The allocator maintains multiple block lists organized by the number of free chunks, allowing efficient selection of partially filled blocks. When no partially filled blocks are available, it calls SlabAllocFromNewBlock to obtain memory from a new block. The function validates that the requested size matches the slab's fixed chunk size and efficiently manages block list transitions as chunks are allocated.

## Parameters / Member Variables
- : The MemoryContext (slab context) from which to allocate memory
- : The size of memory to allocate (must match the slab's fixed chunk size)
- : Allocation flags that control behavior (e.g., MCXT_ALLOC_NO_OOM for NULL return instead of ERROR)

## Dependencies
- Functions called/Symbols referenced:
  - [SlabContext](SlabContext.md)
  - [SlabBlock](SlabBlock.md)
  - MemoryChunk
  - SlabIsValid
  - [SlabBlocklistIndex](SlabBlocklistIndex.md)
  - SlabAllocInvalidSize (when size is invalid)
  - [SlabAllocFromNewBlock](SlabAllocFromNewBlock.md)
  - [dlist_head](../d/dlist_head.md)
  - [dlist_is_empty](../d/dlist_is_empty.md)
  - dlist_head_element
  - [SlabGetNextFreeChunk](SlabGetNextFreeChunk.md)
  - [dlist_delete_from](../d/dlist_delete_from.md)
  - [dlist_push_head](../d/dlist_push_head.md)
  - [SlabFindNextBlockListIndex](SlabFindNextBlockListIndex.md)
  - [SlabAllocSetupNewChunk](SlabAllocSetupNewChunk.md)
- Called from (representative examples):
  - BOGUS_MCTX (src/backend/utils/mmgr/mcxt.c:76)
  - Referenced in MEMUTILS_INTERNAL_H (src/include/utils/memutils_internal.h:57)

## Notes and Other Information
- Optimized for performance with fast-path logic in the main function and slow-path operations in helper functions
- Maintains block lists organized by free chunk count to enable efficient allocation patterns
- Enforces fixed-size allocation constraint by validating that requested size matches slab's chunk size
- Uses unlikely() hints to optimize for the common case of allocating from partially filled blocks
- Handles block list management automatically, moving blocks between lists as their free chunk count changes
- Returns NULL instead of raising ERROR when MCXT_ALLOC_NO_OOM flag is set
- Part of PostgreSQL's specialized memory context system for high-frequency, same-size allocations
- Located in src/backend/utils/mmgr/slab.c:630-700