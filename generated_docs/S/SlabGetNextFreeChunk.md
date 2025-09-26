# SlabGetNextFreeChunk

## Location
[src/backend/utils/mmgr/slab.c:271-321](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/slab.c#L271-L321)

## Overview
SlabGetNextFreeChunk retrieves the next available memory chunk from a slab block and updates the block's internal state to reflect that the chunk is now in use.

## Definition
```c
static inline MemoryChunk *SlabGetNextFreeChunk(SlabContext *slab, SlabBlock *block)
```

## Detailed Description
This function implements the core chunk allocation logic within a slab block. It handles two scenarios for obtaining a free chunk: first, it attempts to reuse a previously freed chunk from the freehead linked list, and if no freed chunks are available, it allocates from the pool of unused chunks. When reusing freed chunks, the function maintains a linked list where each free chunk stores a pointer to the next free chunk in its own memory space. When allocating from unused chunks, it simply advances the unused pointer by the full chunk size. The function includes Valgrind integration for memory debugging and comprehensive assertions to verify memory integrity.

## Parameters / Member Variables
- `slab`: Pointer to the SlabContext containing configuration information including chunk size and counts
- `block`: Pointer to the SlabBlock from which to allocate a chunk

## Dependencies
- Functions called/Symbols referenced:
  - [SlabContext](SlabContext.md) (struct type)
  - [SlabBlock](SlabBlock.md) (struct type)
  - [MemoryChunk](../M/MemoryChunk.md) (struct type)
  - VALGRIND_MAKE_MEM_DEFINED (Valgrind macro for memory debugging)
  - SlabChunkGetPointer (function to get pointer from chunk)
  - SlabBlockGetChunk (function to get specific chunk from block)
  - SlabChunkMod (function for chunk alignment validation)
- Called from (representative examples):
  - [SlabAllocFromNewBlock](SlabAllocFromNewBlock.md)
  - [SlabAlloc](SlabAlloc.md)

## Notes and Other Information
- This is a static inline function for performance optimization
- The function prioritizes reusing freed chunks over unused chunks to improve memory locality
- Freed chunks form a linked list where each chunk stores the pointer to the next free chunk
- Includes comprehensive assertions for memory safety and debugging
- Integrates with Valgrind for memory error detection in debug builds
- Updates both nfree (total free chunks) and nunused (unused chunks) counters appropriately
- The function assumes block->nfree > 0 and will assert if this precondition is violated