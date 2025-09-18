# SlabContext

## Location
src/backend/utils/mmgr/slab.c: 103 - 130

## Overview
SlabContext is a specialized implementation of MemoryContext designed for efficient allocation of fixed-size memory chunks, optimizing memory management for scenarios where many objects of the same size need to be allocated and freed.

## Definition


## Detailed Description
SlabContext implements a slab allocator, which is a memory management technique that pre-allocates memory in blocks and divides them into fixed-size chunks. This approach is particularly efficient for allocating many objects of the same size, as it eliminates memory fragmentation and reduces allocation overhead. The context maintains multiple lists of blocks categorized by their free space availability, enabling fast allocation and deallocation operations.

The slab allocator organizes memory into blocks, where each block contains multiple chunks of the specified size. Blocks are categorized into different lists based on how many free chunks they contain, allowing the allocator to quickly find blocks with available space.

## Parameters / Member Variables
- : Standard MemoryContextData fields inherited from the base memory context interface
- : The requested size of each chunk (before alignment and headers are added)
- : The actual size of each chunk including chunk headers and alignment padding
- : The total size allocated for each block containing multiple chunks
- : The number of individual chunks that can fit within a single block
- : Index into the blocklist array pointing to the list containing the fullest blocks with available space
- : Debug array used during memory checking to track which chunks are free (only present when MEMORY_CONTEXT_CHECKING is enabled)
- : Number of bits to right-shift the free chunk count to calculate the appropriate blocklist index
- : List of completely empty blocks that can be reused before allocating new blocks
- : Array of SLAB_BLOCKLIST_COUNT (3) lists organizing blocks by their free space availability

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextData](../M/MemoryContextData.md)
  - [dclist_head](../d/dclist_head.md)
  - [dlist_head](../d/dlist_head.md)
  - SLAB_BLOCKLIST_COUNT
  - MEMORY_CONTEXT_CHECKING

- Called from (representative examples):
  - [SlabContextCreate](SlabContextCreate.md)
  - [SlabAlloc](SlabAlloc.md)
  - [SlabFree](SlabFree.md)
  - [SlabReset](SlabReset.md)
  - [SlabCheck](SlabCheck.md)
  - [SlabStats](SlabStats.md)

## Notes and Other Information
- The slab allocator is optimized for scenarios with many allocations of the same size, such as tuple storage or node structures
- The blocklist array partitions blocks into categories based on free space, with completely full blocks in blocklist[0]
- Empty blocks are maintained separately in the emptyblocks list to enable quick reuse
- The context provides O(1) allocation and deallocation performance for fixed-size chunks
- Memory checking support is conditionally compiled for debugging purposes
- The design minimizes memory fragmentation compared to general-purpose allocators