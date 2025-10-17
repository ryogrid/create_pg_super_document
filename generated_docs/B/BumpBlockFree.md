# BumpBlockFree

## Location
[src/backend/utils/mmgr/bump.c:595-616](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/bump.c#L595-L616)

## Overview
Removes a BumpBlock from a BumpContext and releases all memory consumed by the block, ensuring proper cleanup and memory accounting.

## Definition

```c
static inline void
BumpBlockFree(BumpContext *set, BumpBlock *block)
```
## Detailed Description
BumpBlockFree is a static inline function that safely removes and deallocates a memory block from a bump memory context. The function performs several critical operations: it validates that the block being freed is not a keeper block (which has special lifecycle management), removes the block from the doubly-linked list of blocks, updates the memory accounting statistics, optionally wipes the memory contents for debugging purposes, and finally releases the memory back to the system using free(). This function is essential for proper memory management within the bump allocator.

## Parameters / Member Variables
- `set`: Pointer to the BumpContext from which the block should be removed
- `block`: Pointer to the BumpBlock to be freed and deallocated

## Dependencies
- Functions called/Symbols referenced:
  - [BumpContext](BumpContext.md) (structure type)
  - [BumpBlock](BumpBlock.md) (structure type)
  - IsKeeperBlock (validation function)
  - [dlist_delete](../d/dlist_delete.md) (linked list removal)
  - [wipe_mem](../w/wipe_mem.md) (memory debugging function, conditional)
- Called from (representative examples):
  - ExternalChunkGetBlock
  - [BumpReset](BumpReset.md)

## Notes and Other Information
- Static inline function for performance, only visible within bump.c
- Includes assertion to prevent accidental freeing of keeper blocks, which have special management rules
- Updates the memory context's mem_allocated counter to maintain accurate memory usage statistics
- Conditionally wipes freed memory when CLOBBER_FREED_MEMORY is defined, helping detect use-after-free bugs during debugging
- Part of PostgreSQL's bump memory allocator designed for scenarios with frequent allocations and infrequent frees

## Simplified Source

```c
static inline void
BumpBlockFree(BumpContext *set, BumpBlock *block)
{
    // Ensure we don't free the keeper block
    Assert(!IsKeeperBlock(set, block));

    // Remove block from linked list
    dlist_delete(&block->node);

    // Update memory accounting
    ((MemoryContext) set)->mem_allocated -= ((char *) block->endptr - (char *) block);

    // Wipe memory for debugging
#ifdef CLOBBER_FREED_MEMORY
    wipe_mem(block, ((char *) block->endptr - (char *) block));
#endif

    // Release memory
    free(block);
}
```