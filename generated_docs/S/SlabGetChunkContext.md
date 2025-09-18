# SlabGetChunkContext

## Location
[src/backend/utils/mmgr/slab.c:863-886](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/slab.c#L863-L886)

## Overview
SlabGetChunkContext retrieves the MemoryContext that owns a given memory chunk allocated by the slab allocator.

## Definition
```c
MemoryContext SlabGetChunkContext(void *pointer)
```

## Detailed Description
SlabGetChunkContext is a utility function that returns the MemoryContext associated with a memory chunk allocated from a slab allocator. The function performs the following operations:

1. **Chunk Header Retrieval**: Converts the user pointer to its corresponding MemoryChunk header
2. **Memory Access Control**: Temporarily allows access to the chunk header for inspection using VALGRIND macros
3. **Block Resolution**: Retrieves the SlabBlock that contains the chunk
4. **Validation**: Asserts that the block is valid and properly formatted
5. **Context Return**: Returns the MemoryContext header from the slab that owns the block

This function is essential for memory management operations that need to identify which memory context owns a particular allocation, enabling proper cleanup and context-aware operations.

## Parameters / Member Variables
- `pointer`: A memory pointer previously allocated by SlabAlloc within a slab context

## Dependencies
- Functions called/Symbols referenced:
  - PointerGetMemoryChunk
  - [MemoryChunkGetBlock](../M/MemoryChunkGetBlock.md)
  - SlabBlockIsValid
  - VALGRIND_MAKE_MEM_DEFINED
  - VALGRIND_MAKE_MEM_NOACCESS
- Called from (representative examples):
  - Memory context introspection routines
  - Debugging and memory analysis tools

## Notes and Other Information
- Uses VALGRIND macros to control memory access during debugging for detecting invalid memory access
- Returns a pointer to the MemoryContext header embedded within the SlabContext structure
- Essential for the memory context hierarchy and debugging capabilities
- Assumes the pointer was allocated by a slab allocator (verified by assertion)