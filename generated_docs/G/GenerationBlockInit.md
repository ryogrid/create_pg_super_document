# GenerationBlockInit

## Location
src/backend/utils/mmgr/generation.c: 609 - 629

## Overview
A static inline function that initializes a newly allocated memory block structure with proper metadata and boundary pointers for the generation memory context.

## Definition
```c
static inline void
GenerationBlockInit(GenerationContext *context, GenerationBlock *block, Size blksize)
```

## Detailed Description
GenerationBlockInit performs the essential initialization of a GenerationBlock structure after memory has been allocated from the system. The function sets up all the necessary metadata including context reference, block size, chunk counters, and calculates the free and end pointers that define the allocatable region within the block. It also handles valgrind integration by marking the unallocated space as NOACCESS for memory debugging purposes.

The function establishes the memory layout by setting freeptr to point immediately after the block header (Generation_BLOCKHDRSZ bytes from the block start) and endptr to the end of the allocated block. This creates a clear boundary for chunk allocation within the block. The function does not update the context's total memory accounting, leaving that responsibility to the caller.

## Parameters / Member Variables
- `context`: Pointer to the GenerationContext that owns this block
- `block`: Pointer to the GenerationBlock structure to initialize
- `blksize`: Total size of the allocated block including headers

## Dependencies
- Functions called/Symbols referenced:
  - Generation_BLOCKHDRSZ (constant defining block header size)
  - VALGRIND_MAKE_MEM_NOACCESS (marks memory as inaccessible for debugging)
- Called from (representative examples):
  - IsKeeperBlock (when setting up keeper blocks)
  - GenerationContextCreate (during context initialization)
  - GenerationAllocFromNewBlock (when creating new blocks)

## Notes and Other Information
- Marked as static inline for performance optimization due to frequent usage
- Does not update context's mem_allocated field - caller responsibility
- Sets initial chunk counters (nchunks and nfree) to zero
- Establishes block memory layout with proper pointer arithmetic
- Integrates with valgrind for memory debugging support
- Critical for proper block lifecycle management in generation contexts
- Block header consumes Generation_BLOCKHDRSZ bytes at the beginning of each block