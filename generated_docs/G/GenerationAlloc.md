# GenerationAlloc

## Location
[src/backend/utils/mmgr/generation.c:527-608](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/generation.c#L527-L608)

## Overview
The main allocation function for the generation memory context that efficiently allocates memory chunks using a strategy optimized for FIFO (First-In-First-Out) workloads with block reuse and fragmentation avoidance.

## Definition
```c
void *
GenerationAlloc(MemoryContext context, Size size, int flags)
```

## Detailed Description
GenerationAlloc is the primary allocation interface for generation memory contexts, designed to provide efficient memory allocation for workloads with FIFO characteristics. The function implements a sophisticated allocation strategy that prioritizes the current block, falls back to reusing empty free blocks, and only allocates new blocks when necessary. This approach minimizes fragmentation and reduces the overhead of frequent system malloc calls.

The function handles allocation requests up to a configurable chunk limit, delegating larger allocations to GenerationAllocLarge(). It employs a careful block selection strategy to avoid ping-ponging between blocks, which could cause fragmentation in FIFO workloads. The implementation is optimized for performance, keeping the common allocation path lean by delegating complex operations to helper functions.

## Parameters / Member Variables
- `context`: The memory context from which to allocate (cast to GenerationContext)
- `size`: The requested allocation size in bytes
- `flags`: Allocation control flags (e.g., MCXT_ALLOC_NO_OOM for non-throwing allocation)

## Dependencies
- Functions called/Symbols referenced:
  - GenerationIsValid (validates context structure)
  - MAXALIGN (aligns size to platform requirements)
  - [GenerationAllocLarge](GenerationAllocLarge.md) (handles oversized allocations)
  - [GenerationBlockFreeBytes](GenerationBlockFreeBytes.md) (checks available space in blocks)
  - GenerationBlockIsEmpty (verifies block emptiness)
  - [GenerationAllocChunkFromBlock](GenerationAllocChunkFromBlock.md) (allocates chunk from existing block)
  - [GenerationAllocFromNewBlock](GenerationAllocFromNewBlock.md) (creates new block and allocates)
- Called from (representative examples):
  - [GenerationRealloc](GenerationRealloc.md) (for reallocation operations)
  - BOGUS_MCTX (memory context interface)
  - MEMUTILS_INTERNAL_H (memory utilities interface)

## Notes and Other Information
- Optimized for performance with minimal stack frame overhead in common cases
- Supports memory context checking mode with sentinel bytes
- Maximum allocation size is MAXALIGN_DOWN(SIZE_MAX) - Generation_BLOCKHDRSZ - Generation_CHUNKHDRSZ
- Implements smart block reuse strategy to minimize fragmentation in FIFO workloads
- Falls back to freeblock reuse before allocating new blocks
- Compatible with valgrind memory checking tools
- Critical performance path - any changes should consider allocation overhead impact