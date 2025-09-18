# SlabBlock

## Location
[src/backend/utils/mmgr/slab.c:146-154](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/slab.c#L146-L154)

## Overview
SlabBlock represents a single block of memory within a SlabContext, containing multiple fixed-size chunks that can be allocated and freed individually.

## Definition


## Detailed Description
SlabBlock is the fundamental unit of memory organization within the slab allocator. Each block contains a fixed number of equally-sized chunks that can be allocated to satisfy memory requests. The block maintains separate tracking for unused chunks (never allocated) and freed chunks (previously allocated but now available for reuse). This dual tracking mechanism allows the allocator to efficiently manage memory lifecycle and implement optimal allocation strategies.

The block uses a freelist data structure to link together freed chunks, enabling constant-time allocation of previously used memory. Unused chunks are tracked with a simple pointer advancement mechanism. The block is linked into the appropriate category list within the SlabContext based on its current free space availability.

## Parameters / Member Variables
- : Pointer back to the owning SlabContext that manages this block
- : Total count of available chunks, including both freed chunks and unused chunks (nfree = number of freed chunks + nunused)
- : Count of chunks that have never been allocated and are available for first-time use
- : Pointer to the first chunk in the freelist of previously allocated but now freed chunks; freed chunks form a linked list using their own memory to store next pointers
- : Pointer to the next chunk that has never been allocated; this advances linearly through the block as chunks are first allocated
- : Doubly-linked list node used to chain this block into one of the SlabContext's blocklist arrays based on free space availability

## Dependencies
- Functions called/Symbols referenced:
  - [SlabContext](SlabContext.md)
  - MemoryChunk
  - [dlist_node](../d/dlist_node.md)

- Called from (representative examples):
  - [SlabGetNextFreeChunk](SlabGetNextFreeChunk.md)
  - [SlabAllocFromNewBlock](SlabAllocFromNewBlock.md)
  - [SlabAlloc](SlabAlloc.md)
  - [SlabFree](SlabFree.md)
  - [SlabReset](SlabReset.md)
  - [SlabStats](SlabStats.md)
  - [SlabCheck](SlabCheck.md)

## Notes and Other Information
- Each block is categorized into different blocklists within the SlabContext based on the number of free chunks it contains
- The freelist mechanism reuses freed chunks before allocating from unused chunks, improving memory locality
- Completely full blocks (nfree = 0) are stored in blocklist[0] and completely empty blocks are moved to the emptyblocks list
- The block structure enables efficient chunk allocation with O(1) performance
- Memory chunks within a block are accessed through pointer arithmetic based on the fixed chunk size
- The dlist_node allows blocks to be efficiently moved between different category lists as their free space changes