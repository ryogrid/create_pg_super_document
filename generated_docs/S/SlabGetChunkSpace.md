# SlabGetChunkSpace

## Location
[src/backend/utils/mmgr/slab.c:887-911](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/slab.c#L887-L911)

## Overview
SlabGetChunkSpace determines the total space occupied by an allocated chunk, including all memory allocation overhead.

## Definition
```c
Size SlabGetChunkSpace(void *pointer)
```

## Detailed Description
SlabGetChunkSpace calculates the complete memory footprint of a chunk allocated from a slab allocator. Unlike functions that return just the usable size, this function returns the total space including overhead such as chunk headers and alignment padding. The function performs the following operations:

1. **Chunk Resolution**: Converts the user pointer to its MemoryChunk header representation
2. **Memory Access Management**: Uses VALGRIND macros to temporarily allow access to chunk headers during inspection
3. **Block Retrieval**: Obtains the SlabBlock containing the chunk
4. **Validation**: Asserts that the block is valid and properly formatted
5. **Size Calculation**: Returns the fullChunkSize from the slab context, which includes all overhead

This function is particularly useful for memory profiling, debugging, and accounting operations where the total memory consumption needs to be accurately measured.

## Parameters / Member Variables
- `pointer`: A memory pointer previously allocated by SlabAlloc

## Dependencies
- Functions called/Symbols referenced:
  - PointerGetMemoryChunk
  - [MemoryChunkGetBlock](../M/MemoryChunkGetBlock.md)
  - SlabBlockIsValid
  - VALGRIND_MAKE_MEM_DEFINED
  - VALGRIND_MAKE_MEM_NOACCESS
- Called from (representative examples):
  - Memory profiling utilities
  - Memory usage reporting functions

## Notes and Other Information
- Returns fullChunkSize which includes chunk header and alignment overhead, not just the usable space
- Essential for accurate memory accounting and profiling in PostgreSQL
- Uses VALGRIND macros for proper memory debugging support
- The returned size is consistent across all chunks in the same slab context due to fixed-size allocation