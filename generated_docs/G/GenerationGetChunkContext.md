# GenerationGetChunkContext

## Location
src/backend/utils/mmgr/generation.c: 947 - 972

## Overview
GenerationGetChunkContext retrieves the MemoryContext that owns a given memory pointer, providing reverse lookup functionality within the Generation allocator.

## Definition
```c
MemoryContext GenerationGetChunkContext(void *pointer)
```

## Detailed Description
This function provides essential introspection capability for the Generation memory allocator by determining which memory context owns a specific memory pointer. The implementation:

1. **Chunk Header Access**: Temporarily enables access to the chunk header for analysis while maintaining memory protection through Valgrind annotations
2. **Block Resolution**: Determines whether the chunk is external or internal and retrieves the appropriate block pointer
3. **Context Retrieval**: Returns the memory context header from the containing block
4. **Memory Protection**: Restores memory access protection to the chunk header after analysis

The function is crucial for debugging, memory tracking, and various PostgreSQL internal operations that need to determine memory ownership. It handles both external chunks (large allocations that span entire blocks) and internal chunks (smaller allocations within shared blocks) transparently.

The implementation includes validation through GenerationBlockIsValid to ensure data integrity and prevent crashes from corrupted memory structures.

## Parameters / Member Variables
- `pointer`: Memory pointer for which to find the owning MemoryContext

## Dependencies
- Functions called/Symbols referenced:
  - PointerGetMemoryChunk - converts pointer to chunk header
  - [MemoryChunkIsExternal](../M/MemoryChunkIsExternal.md) - determines if chunk is external
  - ExternalChunkGetBlock - gets block for external chunks  
  - [MemoryChunkGetBlock](../M/MemoryChunkGetBlock.md) - gets block for internal chunks
  - GenerationBlockIsValid - validates block structure
  - VALGRIND_MAKE_MEM_DEFINED / VALGRIND_MAKE_MEM_NOACCESS - memory access control
- Called from:
  - BOGUS_MCTX - as part of memory context method table
  - Various PostgreSQL internals requiring context identification

## Notes and Other Information
- Essential for memory context introspection and debugging operations
- Provides unified interface for both external and internal chunk types
- Careful Valgrind integration ensures proper memory access tracking during development
- Block validation prevents crashes from corrupted memory structures
- Returns the actual MemoryContext header rather than the Generation-specific context structure
- Used by PostgreSQL's memory context debugging and analysis tools
- Thread-safe operation through proper memory access patterns