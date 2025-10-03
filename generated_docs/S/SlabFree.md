# SlabFree

## Location
[src/backend/utils/mmgr/slab.c:701-825](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/slab.c#L701-L825)

## Overview
SlabFree deallocates memory chunks from the slab memory context, managing block lists and potentially freeing or reusing empty blocks to optimize memory usage.

## Definition

```c
void
SlabFree(void *pointer)
```
## Detailed Description
SlabFree is the deallocation function for the slab memory allocator in PostgreSQL. It performs the following key operations:

1. **Chunk Validation**: Retrieves the memory chunk header from the pointer and validates the associated slab block
2. **Memory Checking**: In debug builds, verifies that no memory corruption occurred by checking sentinels
3. **Free List Management**: Adds the freed chunk to the block's free list by updating the freehead pointer
4. **Block List Reorganization**: Moves blocks between different block lists based on their new free chunk count
5. **Empty Block Handling**: When a block becomes completely empty, either adds it to a reuse pool or frees it entirely
6. **Index Updates**: Maintains the slab's current block list index to ensure efficient allocation

The function uses sophisticated block list management to maintain performance by keeping blocks sorted by utilization levels.

## Parameters / Member Variables
- `*pointer`: The memory pointer to be freed, previously allocated by SlabAlloc
## Dependencies
- Functions called/Symbols referenced:
  - PointerGetMemoryChunk
  - [MemoryChunkGetBlock](../M/MemoryChunkGetBlock.md)
  - SlabBlockIsValid
  - [sentinel_ok](../s/sentinel_ok.md) (debug builds)
  - [SlabBlocklistIndex](SlabBlocklistIndex.md)
  - [dlist_delete_from](../d/dlist_delete_from.md)
  - [dlist_push_head](../d/dlist_push_head.md)
  - [SlabFindNextBlockListIndex](SlabFindNextBlockListIndex.md)
  - [dclist_count](../d/dclist_count.md)
  - [dclist_push_head](../d/dclist_push_head.md)
  - [wipe_mem](../w/wipe_mem.md) (debug builds)
- Called from (representative examples):
  - Memory context free operations
  - Slab context cleanup routines

## Notes and Other Information
- Uses VALGRIND_MAKE_MEM_DEFINED for memory debugging support
- Implements sophisticated block list management with multiple utilization levels
- Maintains a pool of empty blocks (up to SLAB_MAXIMUM_EMPTY_BLOCKS) to avoid malloc/free thrashing
- Includes extensive memory corruption detection in debug builds
- Critical for maintaining slab allocator performance through proper block organization