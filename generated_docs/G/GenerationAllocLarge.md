# GenerationAllocLarge

## Location
[src/backend/utils/mmgr/generation.c:343-412](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/generation.c#L343-L412)

## Overview
Allocates an entire dedicated block for a single large chunk in a Generation memory context, used when the requested size exceeds the context's chunk allocation limit.

## Definition

```c
static void *
GenerationAllocLarge(MemoryContext context, Size size, int flags)
```
## Detailed Description
GenerationAllocLarge is a specialized allocation function that handles large memory requests by creating a dedicated block containing a single chunk. This function is called when the requested allocation size exceeds the context's allocChunkLimit, making it inefficient to allocate from regular blocks that contain multiple smaller chunks.

The function creates a block sized exactly for the requested chunk plus necessary headers, marks the chunk as externally managed, and adds the block to the context's block list. It includes comprehensive memory debugging support with sentinel bytes, memory randomization, and Valgrind annotations to help detect memory errors.

## Parameters / Member Variables
- `context`: The Generation memory context to allocate from
- `size`: The number of bytes to allocate
- `flags`: Allocation flags that control behavior and validation
## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextCheckSize
  - malloc
  - [MemoryContextAllocationFailure](../M/MemoryContextAllocationFailure.md)
  - [MemoryChunkSetHdrMaskExternal](../M/MemoryChunkSetHdrMaskExternal.md)
  - [set_sentinel](../s/set_sentinel.md) (when MEMORY_CONTEXT_CHECKING)
  - [randomize_mem](../r/randomize_mem.md) (when RANDOMIZE_ALLOCATED_MEMORY)
  - [dlist_push_head](../d/dlist_push_head.md)
  - MemoryChunkGetPointer
  - VALGRIND_MAKE_MEM_NOACCESS
- Called from (representative examples):
  - [GenerationAlloc](GenerationAlloc.md)

## Notes and Other Information
- This function is marked pg_noinline to keep GenerationAlloc() small and fast for common cases
- Creates a dedicated block with exactly one chunk (nchunks=1, nfree=0)
- The block is immediately full as freeptr equals endptr
- Includes memory debugging features like sentinel bytes and memory randomization
- Uses Valgrind annotations to mark padding and headers as NOACCESS for memory error detection
- The chunk is marked as externally managed with MCTX_GENERATION_ID
- Block size calculation includes chunk data, chunk header, and block header
- Memory allocation failure is handled through MemoryContextAllocationFailure