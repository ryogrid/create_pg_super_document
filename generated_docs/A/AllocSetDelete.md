# AllocSetDelete

## Location
[src/backend/utils/mmgr/aset.c:607-695](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/aset.c#L607-L695)

## Overview
Completely destroys an AllocSet memory context by freeing all allocated memory, with optimizations for context recycling through freelists to improve performance.

## Definition


## Detailed Description
AllocSetDelete provides complete destruction of an AllocSet memory context, ensuring all memory resources are properly freed. Unlike AllocSetReset which preserves the keeper block, this function must free all memory associated with the context.

The function implements an important performance optimization through context recycling. For contexts that match standard size configurations (indicated by freeListIndex >= 0), instead of immediately destroying the context, it attempts to place the context into a global freelist for later reuse by AllocSetContextCreateInternal.

The recycling process involves:
1. Resetting the context if needed to free non-keeper blocks
2. Managing freelist capacity by discarding old contexts when full
3. Adding the current context to the appropriate freelist

For contexts that cannot be recycled (freeListIndex < 0), it performs traditional destruction by freeing all blocks individually, including the final free() of the context header and keeper block.

## Parameters / Member Variables
- : The AllocSet memory context to delete (cast internally to AllocSet)

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetIsValid (context validation)
  - [AllocSetCheck](AllocSetCheck.md) (optional corruption/leak checking)
  - KeeperBlock (macro to access initial block)
  - IsKeeperBlock (tests if block is the keeper block)
  - [MemoryContextResetOnly](../M/MemoryContextResetOnly.md) (resets context without calling callbacks)
  - [wipe_mem](../w/wipe_mem.md) (memory wiping for debugging)
  - free (system memory deallocation)

- Referenced constants/macros:
  - MEMORY_CONTEXT_CHECKING (debug build option)
  - CLOBBER_FREED_MEMORY (memory debugging option)
  - MAX_FREE_CONTEXTS (maximum contexts per freelist)
  - PG_USED_FOR_ASSERTS_ONLY (assertion-only variable marking)

- Data structures:
  - [AllocSetFreeList](AllocSetFreeList.md) (freelist management structure)
  - context_freelists (global freelist array)

- Called from (representative examples):
  - Memory context deletion operations through function pointers
  - Context management routines in mcxt.c

## Notes and Other Information
- Context recycling optimization: Places eligible contexts into freelists for reuse rather than immediate destruction
- Freelist management: Maintains capacity limits and discards old contexts when freelists become full
- Complete resource cleanup: Unlike reset operations, ensures all memory including the keeper block is eventually freed
- Only contexts with standard size parameters (freeListIndex >= 0) are eligible for recycling
- Includes optional memory corruption and leak detection in debug builds
- Memory debugging support through freed memory wiping when CLOBBER_FREED_MEMORY is enabled
- Maintains accurate mem_allocated tracking throughout the deletion process
- The recycling mechanism reduces malloc/free overhead for frequently created/destroyed contexts
- Assertion validates that only keeper block memory remains before final context header free
- Freelist overflow handling prevents unbounded memory usage by discarding excess cached contexts