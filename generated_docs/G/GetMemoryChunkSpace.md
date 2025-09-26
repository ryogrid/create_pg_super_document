# GetMemoryChunkSpace

## Location
src/backend/utils/mmgr/mcxt.c: 721 - 730

## Overview
GetMemoryChunkSpace determines the total space occupied by a memory chunk, including all memory allocation overhead.

## Definition
```c
Size GetMemoryChunkSpace(void *pointer)
```

## Detailed Description
GetMemoryChunkSpace is a utility function that calculates the total memory space occupied by an allocated chunk. Unlike functions that return just the requested allocation size, this function includes all memory management overhead such as headers, alignment padding, and any other bookkeeping data maintained by the memory context system.

The function delegates to the memory context's get_chunk_space method via the MCXT_METHOD macro, allowing different memory context implementations to provide their own space calculation logic.

This function is particularly useful for:
- Memory usage analysis and profiling
- Measuring the total footprint of data structures
- Memory management debugging and optimization

## Parameters / Member Variables
- `pointer`: A pointer to a currently allocated memory chunk whose space usage is to be determined

## Dependencies
- Functions called/Symbols referenced:
  - MCXT_METHOD (macro for accessing memory context methods)
- Called from (representative examples):
  - ginCombineData (GIN index bulk operations)
  - tuplesort_begin_batch (tuple sorting operations)
  - tuplestore_begin_common (tuple store management)
  - AlignedAllocGetChunkSpace (aligned memory allocator)

## Notes and Other Information
- Returns the total space as a Size type (typically size_t)
- The pointer must be a valid, currently allocated chunk - behavior with freed or invalid pointers is undefined
- Different memory context implementations may calculate overhead differently
- This function is read-only and does not modify the memory chunk or its context
- Commonly used in memory accounting and debugging scenarios where precise memory usage tracking is required