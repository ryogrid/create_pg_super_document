# SlabAlloc

## Location
[src/backend/utils/mmgr/slab.c:630-700](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/slab.c#L630-L700)

## Overview
SlabAlloc is the main allocation function for the slab memory allocator that returns a pointer to a newly allocated memory chunk of a fixed size.

## Definition

```c
void *
SlabAlloc(MemoryContext context, Size size, int flags)
```
## Detailed Description
SlabAlloc is the primary allocation function for PostgreSQL's slab memory allocator, designed for high-performance allocation of fixed-size memory chunks. The function implements a fast-path optimization strategy: it handles the most common allocation scenarios in the main function body to avoid stack frame overhead, while delegating edge cases to helper functions. The allocator maintains multiple block lists organized by the number of free chunks, allowing efficient selection of partially filled blocks. When no partially filled blocks are available, it calls SlabAllocFromNewBlock to obtain memory from a new block. The function validates that the requested size matches the slab's fixed chunk size and efficiently manages block list transitions as chunks are allocated.

## Parameters / Member Variables
- `context`: The MemoryContext (slab context) from which to allocate memory
- `size`: The size of memory to allocate (must match the slab's fixed chunk size)
- `flags`: Allocation flags that control behavior (e.g., MCXT_ALLOC_NO_OOM for NULL return instead of ERROR)
## Dependencies
- Functions called/Symbols referenced:
  - [SlabContext](SlabContext.md)
  - [SlabBlock](SlabBlock.md)
  - [MemoryChunk](../M/MemoryChunk.md)
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

## Simplified Source

```c
void *
SlabAlloc(MemoryContext context, Size size, int flags)
{
    SlabContext *slab = (SlabContext *) context;
    SlabBlock *block;
    MemoryChunk *chunk;

    Assert(SlabIsValid(slab));

    // Validate blocklist index
    Assert(slab->curBlocklistIndex >= 0);
    Assert(slab->curBlocklistIndex <= SlabBlocklistIndex(slab, slab->chunksPerBlock));

    // Ensure requested size matches fixed chunk size
    if (unlikely(size != slab->chunkSize))
        SlabAllocInvalidSize(context, size);

    // Handle case when no partially filled blocks are available
    if (unlikely(slab->curBlocklistIndex == 0))
    {
        return SlabAllocFromNewBlock(context, size, flags);
    }
    else
    {
        dlist_head *blocklist = &slab->blocklist[slab->curBlocklistIndex];
        int new_blocklist_idx;

        Assert(!dlist_is_empty(blocklist));

        // Get block from current blocklist
        block = dlist_head_element(SlabBlock, node, blocklist);

        Assert(block != NULL);
        Assert(slab->curBlocklistIndex == SlabBlocklistIndex(slab, block->nfree));
        Assert(block->nfree > 0);

        // Get next free chunk from this block
        chunk = SlabGetNextFreeChunk(slab, block);

        // Calculate new blocklist index based on updated free chunk count
        new_blocklist_idx = SlabBlocklistIndex(slab, block->nfree);

        // Move block to new list if its free count category changed
        if (unlikely(slab->curBlocklistIndex != new_blocklist_idx))
        {
            dlist_delete_from(blocklist, &block->node);
            dlist_push_head(&slab->blocklist[new_blocklist_idx], &block->node);

            // Update current index if current blocklist became empty
            if (dlist_is_empty(blocklist))
                slab->curBlocklistIndex = SlabFindNextBlockListIndex(slab);
        }
    }

    return SlabAllocSetupNewChunk(context, block, chunk, size);
}
```