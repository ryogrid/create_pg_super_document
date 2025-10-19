# GenerationGetChunkSpace

## Location
[src/backend/utils/mmgr/generation.c:973-1001](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/generation.c#L973-L1001)

## Overview
GenerationGetChunkSpace calculates the total memory space occupied by a memory chunk, including both the user data and all memory allocation overhead.

## Definition
```c
Size GenerationGetChunkSpace(void *pointer)
```

## Detailed Description
This function provides precise memory usage accounting for allocated chunks within the Generation memory context. It determines the complete memory footprint including:

1. **User Data Space**: The actual data area available to the application
2. **Header Overhead**: The chunk header space (Generation_CHUNKHDRSZ bytes)
3. **Allocation Overhead**: Any additional space consumed by the allocation mechanism

The implementation distinguishes between two chunk types:

- **External Chunks**: Large allocations that occupy entire blocks, where size is calculated as the difference between the block's end pointer and the data start
- **Internal Chunks**: Smaller allocations within shared blocks, where size is stored directly in the chunk header

The function is essential for memory accounting, debugging, and optimization analysis, providing accurate memory usage statistics that include all overhead costs, not just the requested allocation size.

## Parameters / Member Variables
- `pointer`: Pointer to an allocated memory chunk

## Dependencies
- Functions called/Symbols referenced:
  - PointerGetMemoryChunk - converts pointer to chunk header
  - [MemoryChunkIsExternal](../M/MemoryChunkIsExternal.md) - determines chunk allocation type
  - ExternalChunkGetBlock - retrieves block for external chunks
  - [MemoryChunkGetValue](../M/MemoryChunkGetValue.md) - gets stored size for internal chunks
  - GenerationBlockIsValid - validates block structure integrity
  - VALGRIND_MAKE_MEM_DEFINED / VALGRIND_MAKE_MEM_NOACCESS - memory access control
- Called from:
  - BOGUS_MCTX - as part of memory context method table
  - Memory usage analysis and debugging tools throughout PostgreSQL

## Notes and Other Information
- Returns total memory consumption including all overhead, not just usable space
- Handles both external and internal chunk types with appropriate size calculation methods
- Essential for accurate memory accounting and leak detection
- Valgrind integration ensures proper memory access control during development
- Used by PostgreSQL's memory usage reporting and analysis tools
- The returned size always includes the Generation_CHUNKHDRSZ overhead
- Provides foundation for memory context statistics and optimization decisions
- Thread-safe operation through careful memory access patterns

## Simplified Source

```c
Size GenerationGetChunkSpace(void *pointer) {
    MemoryChunk *chunk = PointerGetMemoryChunk(pointer);
    Size chunksize;

    // Allow access to chunk header for analysis
    VALGRIND_MAKE_MEM_DEFINED(chunk, Generation_CHUNKHDRSZ);

    // Calculate chunk size based on allocation type
    if (MemoryChunkIsExternal(chunk)) {
        // External chunk: size from pointer to block end
        GenerationBlock *block = ExternalChunkGetBlock(chunk);
        chunksize = block->endptr - (char *) pointer;
    }
    else {
        // Internal chunk: size stored in chunk header
        chunksize = MemoryChunkGetValue(chunk);
    }

    // Restore memory protection
    VALGRIND_MAKE_MEM_NOACCESS(chunk, Generation_CHUNKHDRSZ);

    // Return total space including header overhead
    return Generation_CHUNKHDRSZ + chunksize;
}
```