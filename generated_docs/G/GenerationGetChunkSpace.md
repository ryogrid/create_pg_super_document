# GenerationGetChunkSpace

## Location
src/backend/utils/mmgr/generation.c: 973 - 1001

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
  - MemoryChunkIsExternal - determines chunk allocation type
  - ExternalChunkGetBlock - retrieves block for external chunks
  - MemoryChunkGetValue - gets stored size for internal chunks
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