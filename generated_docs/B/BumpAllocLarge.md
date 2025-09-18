# BumpAllocLarge

## Location
[src/backend/utils/mmgr/bump.c:293-370](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/bump.c#L293-L370)

## Overview
Handles large memory allocations in Bump contexts by creating dedicated blocks that contain only a single large chunk.

## Definition


## Detailed Description
BumpAllocLarge is a specialized allocation function for requests that exceed the normal chunk size limits in Bump contexts. When a requested allocation is too large to fit efficiently within the standard block allocation strategy, this function creates a dedicated block that contains only the large chunk being allocated. This approach prevents large allocations from fragmenting the regular allocation blocks and ensures that the large allocation doesn't prevent efficient packing of smaller allocations.

The function allocates an entire block sized exactly to hold the requested chunk plus necessary headers, marks the block as completely full, and adds it to the tail of the blocks list. The memory is optionally filled with debugging patterns and protected with memory checking instrumentation when enabled.

## Parameters / Member Variables
- `context`: The Bump memory context to allocate from
- `size`: Size of the requested allocation in bytes
- `flags`: Memory allocation flags controlling behavior

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextCheckSize
  - malloc
  - [MemoryContextAllocationFailure](../M/MemoryContextAllocationFailure.md)
  - [MemoryChunkSetHdrMaskExternal](../M/MemoryChunkSetHdrMaskExternal.md) (in MEMORY_CONTEXT_CHECKING builds)
  - [set_sentinel](../s/set_sentinel.md) (in MEMORY_CONTEXT_CHECKING builds)
  - MemoryChunkGetPointer
  - [randomize_mem](../r/randomize_mem.md) (if RANDOMIZE_ALLOCATED_MEMORY defined)
  - [dlist_push_tail](../d/dlist_push_tail.md)
  - VALGRIND_MAKE_MEM_NOACCESS (in MEMORY_CONTEXT_CHECKING builds)
- Called from (representative examples):
  - [BumpAlloc](BumpAlloc.md)

## Notes and Other Information
- The function is marked pg_noinline to keep the main allocation path (BumpAlloc) lightweight
- Dedicated blocks are added to the tail of the blocks list, preserving the current block at the head for regular allocations
- The block is marked as completely full (freeptr == endptr) since it contains exactly one chunk
- Memory context checking builds include additional overhead for chunk headers, sentinel bytes, and Valgrind integration
- Large allocations are managed as external chunks with special header markings for debugging and validation