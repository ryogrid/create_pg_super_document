# AllocSetReset

## Location
[src/backend/utils/mmgr/aset.c:537-606](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/aset.c#L537-L606)

## Overview
Resets an AllocSet memory context by freeing all allocated memory while preserving the keeper block to avoid malloc thrashing during repeated reset cycles.

## Definition


## Detailed Description
AllocSetReset implements an efficient context reset mechanism that deallocates all memory chunks within an AllocSet context while using a keeper block optimization. Rather than freeing all memory blocks back to the system, it retains the initial "keeper" block that was allocated with the context header.

This design prevents malloc/free thrashing in scenarios where contexts are repeatedly allocated from, reset, and reused - a common pattern in PostgreSQL for per-tuple processing contexts. The keeper block remains available for immediate reuse without requiring new system memory allocation.

The function performs the following operations:
1. Validates the context and optionally checks for memory corruption/leaks
2. Clears all chunk freelists to mark all allocations as freed  
3. Iterates through all blocks, freeing non-keeper blocks to the system
4. Resets the keeper block's free pointer to make its space available
5. Resets the block size allocation sequence to initial values

Memory debugging features ensure freed memory is wiped or marked inaccessible when appropriate build options are enabled.

## Parameters / Member Variables
- : The AllocSet memory context to reset (cast internally to AllocSet)

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetIsValid (context validation)
  - [AllocSetCheck](AllocSetCheck.md) (optional corruption/leak checking)
  - KeeperBlock (macro to access initial block)
  - IsKeeperBlock (tests if block is the keeper block)
  - MemSetAligned (aligned memory clearing)  
  - [wipe_mem](../w/wipe_mem.md) (memory wiping for debugging)
  - VALGRIND_MAKE_MEM_NOACCESS (memory debugging support)
  - free (system memory deallocation)
  
- Referenced constants/macros:
  - ALLOC_BLOCKHDRSZ (block header size)
  - MEMORY_CONTEXT_CHECKING (debug build option)
  - CLOBBER_FREED_MEMORY (memory debugging option)
  - PG_USED_FOR_ASSERTS_ONLY (assertion-only variable marking)

- Called from (representative examples):
  - Memory context reset operations through function pointers
  - Context management routines in mcxt.c

## Notes and Other Information
- Keeper block optimization: Retains the initial block to avoid malloc/free overhead on repeated resets
- The keeper block shares its malloc chunk with the context header, so it cannot be freed independently
- Includes optional memory corruption and leak detection in debug builds (MEMORY_CONTEXT_CHECKING)
- Supports memory debugging through wiping freed memory or marking it inaccessible
- Resets the nextBlockSize to initBlockSize to restart the block size growth sequence
- Maintains accurate mem_allocated tracking by subtracting freed block sizes
- Assertion ensures only keeper block memory remains allocated after reset completion
- Designed specifically for high-frequency reset scenarios like per-tuple context processing