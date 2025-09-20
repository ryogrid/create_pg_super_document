# GenerationReset

## Location
[src/backend/utils/mmgr/generation.c:283-327](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/generation.c#L283-L327)

## Overview
Frees all memory allocated in the given Generation memory context while preserving the initial "keeper" block to avoid malloc thrashing during repeated reset operations.

## Definition

```c
void
GenerationReset(MemoryContext context)
```
## Detailed Description
GenerationReset efficiently clears a Generation memory context by freeing all allocated memory except for the initial "keeper" block. This function is optimized for scenarios where contexts are repeatedly reset after small allocations. The keeper block, which shares a malloc chunk with the context header, is retained and marked as empty rather than being freed back to the operating system.

The function iterates through all blocks in the context's doubly-linked list, distinguishing between the keeper block (which is preserved) and regular blocks (which are freed). It also resets the allocation state to use the keeper block for future allocations and resets the block size allocation sequence back to the initial block size.

## Parameters / Member Variables
- : The Generation memory context to reset (must be a valid GenerationContext)

## Dependencies
- Functions called/Symbols referenced:
  - GenerationIsValid
  - [GenerationCheck](GenerationCheck.md) (when MEMORY_CONTEXT_CHECKING is enabled)
  - dlist_foreach_modify
  - dlist_container
  - IsKeeperBlock
  - [GenerationBlockMarkEmpty](GenerationBlockMarkEmpty.md)
  - [GenerationBlockFree](GenerationBlockFree.md)
  - KeeperBlock
  - [dlist_is_empty](../d/dlist_is_empty.md)
  - [dlist_has_next](../d/dlist_has_next.md)
  - [dlist_head_node](../d/dlist_head_node.md)
- Called from (representative examples):
  - [GenerationDelete](GenerationDelete.md)
  - BOGUS_MCTX
  - MEMUTILS_INTERNAL_H

## Notes and Other Information
- The keeper block optimization prevents malloc/free thrashing in repetitive allocation patterns
- The function includes memory context checking for corruption and leaks when compiled with MEMORY_CONTEXT_CHECKING
- After reset, the freeblock pointer is NULLified to ensure proper state
- The nextBlockSize is reset to initBlockSize to restart the block size growth sequence
- Assertions ensure the context maintains exactly one block (the keeper block) after reset
- The function maintains the invariant that there is always at least one block in the context