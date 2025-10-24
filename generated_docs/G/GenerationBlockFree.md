# GenerationBlockFree

## Location
[src/backend/utils/mmgr/generation.c:664-688](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/generation.c#L664-L688)

## Overview
GenerationBlockFree is a static inline function that removes a memory block from a Generation context and releases the memory consumed by it, ensuring proper cleanup of non-keeper blocks.

## Definition

```c
static inline void
GenerationBlockFree(GenerationContext *set, GenerationBlock *block)
```
## Detailed Description
This function is responsible for safely removing and freeing memory blocks from a Generation memory context. It performs several safety checks before freeing the block:

1. Verifies that the block being freed is not the keeper block (which must persist for the lifetime of the context)
2. Ensures that the block is not the current freeblock (which is actively being used for allocations)
3. Removes the block from the doubly-linked list of blocks
4. Updates the memory accounting by subtracting the block size from the total allocated memory
5. Optionally wipes the memory content if CLOBBER_FREED_MEMORY is defined for debugging
6. Finally releases the block memory back to the system

The function is marked as static inline, indicating it's an internal utility function optimized for performance within the generation memory allocator.

## Parameters / Member Variables
- `set`: Pointer to the GenerationContext from which the block should be removed
- `block`: Pointer to the GenerationBlock to be freed

## Dependencies
- Functions called/Symbols referenced:
  - IsKeeperBlock - checks if the block is a keeper block
  - [dlist_delete](../d/dlist_delete.md) - removes block from doubly-linked list
  - [wipe_mem](../w/wipe_mem.md) - clears memory content (when CLOBBER_FREED_MEMORY is defined)
  - free - system call to release memory
- Called from:
  - [GenerationReset](GenerationReset.md) - [when](../w/when.md) resetting the memory context
  - [GenerationFree](GenerationFree.md) - during memory deallocation operations

## Notes and Other Information
- This function includes important safety assertions to prevent freeing critical blocks
- Memory accounting is properly maintained by updating the mem_allocated field
- The function supports debugging through optional memory wiping
- Being static inline, this function is only accessible within the generation.c file and is optimized for performance
- The keeper block mechanism ensures that there's always at least one block available for allocations

## Simplified Source

```c
static inline void
GenerationBlockFree(GenerationContext *set, GenerationBlock *block)
{
    // Safety checks - don't free critical blocks
    Assert(!IsKeeperBlock(set, block));
    Assert(block != set->freeblock);

    // Remove block from the linked list
    dlist_delete(&block->node);

    // Update memory accounting
    ((MemoryContext) set)->mem_allocated -= block->blksize;

    // Clear memory if debugging enabled
#ifdef CLOBBER_FREED_MEMORY
    wipe_mem(block, block->blksize);
#endif

    // Free the block memory
    free(block);
}
```