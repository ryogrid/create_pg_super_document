# AllocSetContextCreateInternal

## Location
src/backend/utils/mmgr/aset.c: 347 - 536

## Overview
Creates a new AllocSet memory context with specified size parameters, implementing PostgreSQL's primary memory management mechanism with optimized block allocation and freelist management.

## Definition


## Detailed Description
AllocSetContextCreateInternal is the core function for creating PostgreSQL's AllocSet memory contexts. It implements sophisticated memory management by establishing freelists for different allocation sizes, managing memory blocks efficiently, and providing context recycling for improved performance.

The function first attempts to reuse an existing context from freelists (context_freelists[]) if the parameters match standard configurations (default or small). If no suitable context exists for reuse, it allocates a new context structure with an initial memory block.

Key design aspects include:
- Context recycling through freelists for common size configurations
- Initial block sizing based on context requirements vs. requested block size
- Chunk size limit calculation to optimize memory usage and minimize waste
- Integration with PostgreSQL's memory context hierarchy
- Comprehensive parameter validation and alignment checking

## Parameters / Member Variables
- : Parent memory context in the hierarchy, or NULL for top-level contexts
- : Context name (must be statically allocated string for identification/debugging)  
- : Minimum size for the context's initial block (0 = use initBlockSize)
- : Initial allocation block size for the context
- : Maximum size for any single allocation block

## Dependencies
- Functions called/Symbols referenced:
  - StaticAssertDecl (compile-time assertions for alignment)
  - AllocHugeSizeIsValid (validates block size limits)
  - malloc (system memory allocation)
  - [MemoryContextCreate](../M/MemoryContextCreate.md) (generic context initialization)
  - [MemoryContextStats](../M/MemoryContextStats.md) (memory usage reporting on errors)
  - KeeperBlock (macro to access initial block)
  - MemSetAligned (aligned memory initialization)
  - VALGRIND_MAKE_MEM_NOACCESS (memory debugging support)
  
- Referenced constants:
  - ALLOC_CHUNKHDRSZ, ALLOC_BLOCKHDRSZ (header sizes)
  - ALLOCSET_DEFAULT_MINSIZE/INITSIZE, ALLOCSET_SMALL_MINSIZE/INITSIZE
  - ALLOC_CHUNK_LIMIT, ALLOCSET_SEPARATE_THRESHOLD
  - MEMORYCHUNK_MAX_BLOCKOFFSET (addressing limit)

- Called from (representative examples):  
  - AllocSetContextCreate (public wrapper macro)
  - MemoryContextCopyAndSetIdentifier (context duplication)

## Notes and Other Information
- Context recycling optimization: Reuses freed contexts with matching parameters from global freelists
- Supports two standard size configurations (default and small) for common use cases
- Implements power-of-2 chunk size limits to optimize memory block utilization
- Uses keeper blocks where the context header and first data block are contiguous 
- Includes comprehensive compile-time and runtime assertions for parameter validation
- Integrates with Valgrind for memory debugging in development builds
- The allocChunkLimit calculation ensures ~1/8 maximum waste ratio for streaming allocations
- Failure to allocate initial block triggers detailed memory context statistics before error reporting