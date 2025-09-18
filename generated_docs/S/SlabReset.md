# SlabReset

## Location
src/backend/utils/mmgr/slab.c: 431 - 484

## Overview
SlabReset frees all allocated memory in a slab context and resets it to its initial empty state without destroying the context structure itself.

## Definition
```c
void SlabReset(MemoryContext context)
```

## Detailed Description
This function implements a complete reset operation for a slab memory context, freeing all allocated blocks while preserving the context structure for continued use. It operates in two phases: first releasing any retained empty blocks from the emptyblocks list, then walking through all blocklists to free active blocks. The function includes optional memory checking for corruption detection and memory clobbering for debugging. After freeing all blocks, it resets the current blocklist index to 0 and updates the memory accounting to reflect that no memory is allocated. This is more efficient than destroying and recreating the context when you need to clear all allocations.

## Parameters / Member Variables
- `context`: Pointer to the MemoryContext to reset (cast internally to SlabContext)

## Dependencies
- Functions called/Symbols referenced:
  - SlabContext (struct type)
  - dlist_mutable_iter (doubly-linked list iterator type)
  - SlabIsValid (validation function)
  - MEMORY_CONTEXT_CHECKING (conditional compilation macro)
  - SlabCheck (memory checking function)
  - dclist_foreach_modify, dlist_foreach_modify (list iteration macros)
  - SlabBlock (struct type)
  - dlist_container (macro to get container from list node)
  - dclist_delete_from, dlist_delete (list deletion functions)
  - CLOBBER_FREED_MEMORY (conditional compilation macro)
  - wipe_mem (memory clearing function)
  - SLAB_BLOCKLIST_COUNT (constant for number of blocklists)
  - free (system memory deallocation)
- Called from (representative examples):
  - BOGUS_MCTX (memory context framework)
  - SlabDelete
  - MEMUTILS_INTERNAL_H (memory utilities header)

## Notes and Other Information
- This function does not destroy the context itself, only frees allocated blocks
- Includes comprehensive memory checking when MEMORY_CONTEXT_CHECKING is enabled
- Supports memory clobbering for debugging when CLOBBER_FREED_MEMORY is defined
- Maintains accurate memory accounting by updating context->mem_allocated
- Resets curBlocklistIndex to 0 to restart allocation from the beginning
- More efficient than SlabDelete + SlabContextCreate for reusing contexts
- Uses safe iteration macros that allow modification during iteration
- Asserts that all memory is properly freed (mem_allocated == 0) at the end