# SlabAllocFromNewBlock

## Location
[src/backend/utils/mmgr/slab.c:539-604](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/slab.c#L539-L604)

## Overview
SlabAllocFromNewBlock allocates memory from a new slab block when no existing blocks have available chunks, either by reusing an empty block or creating a completely new block.

## Definition

```c
static void *
SlabAllocFromNewBlock(MemoryContext context, Size size, int flags)
```
## Detailed Description
SlabAllocFromNewBlock is called when the slab allocator needs to obtain memory from a new block because all existing blocks are full. The function implements a two-tier strategy: first, it attempts to reuse an empty block from the emptyblocks list if available. If no empty blocks exist, it allocates a completely new block using malloc(). For reused empty blocks, it verifies the block state and retrieves the next free chunk. For new blocks, it initializes the block structure, sets up the first chunk for allocation, and properly initializes the unused chunk tracking. The function then places the block in the appropriate blocklist based on its free chunk count and calls SlabAllocSetupNewChunk to finalize the chunk setup.

## Parameters / Member Variables
- : The MemoryContext (slab context) from which to allocate memory
- : The size of memory to allocate
- : Allocation flags that control behavior (e.g., error handling)

## Dependencies
- Functions called/Symbols referenced:
  - [SlabContext](SlabContext.md)
  - [SlabBlock](SlabBlock.md)
  - [MemoryChunk](../M/MemoryChunk.md)
  - [dclist_count](../d/dclist_count.md)
  - [dclist_pop_head_node](../d/dclist_pop_head_node.md)
  - dlist_container
  - [SlabGetNextFreeChunk](SlabGetNextFreeChunk.md)
  - malloc
  - [MemoryContextAllocationFailure](../M/MemoryContextAllocationFailure.md)
  - SlabBlockGetChunk
  - [SlabBlocklistIndex](SlabBlocklistIndex.md)
  - [dlist_is_empty](../d/dlist_is_empty.md)
  - [dlist_push_head](../d/dlist_push_head.md)
  - [SlabAllocSetupNewChunk](SlabAllocSetupNewChunk.md)
- Called from (representative examples):
  - [SlabAlloc](SlabAlloc.md) (src/backend/utils/mmgr/slab.c:656)

## Notes and Other Information
- Marked with pg_noinline to prevent inlining, likely for debugging or performance profiling purposes
- Implements efficient block reuse by checking the empty blocks list first before allocating new memory
- Properly maintains the slab's blocklist structure by placing the block in the correct list based on free chunk count
- Updates the current blocklist index to optimize future allocations
- Includes assertions to verify block state consistency, particularly for reused empty blocks
- Updates the context's mem_allocated counter when allocating new blocks
- Located in src/backend/utils/mmgr/slab.c:539-604