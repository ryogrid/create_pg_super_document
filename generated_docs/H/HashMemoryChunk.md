# HashMemoryChunk

## Location
[src/include/executor/hashjoin.h:148-149](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/hashjoin.h#L148-L149)

## Overview
HashMemoryChunk is a typedef that provides a convenient pointer type for accessing HashMemoryChunkData structures in PostgreSQL's hash join memory management system.

## Definition

```c
typedef struct HashMemoryChunkData *HashMemoryChunk;
```
## Detailed Description
HashMemoryChunk serves as a standardized pointer type for working with memory chunks in PostgreSQL's hash join implementation. This typedef abstracts the underlying pointer semantics and provides a clean interface for functions that manipulate the chunked memory allocation system used to store hash join tuples efficiently.

By using this typedef instead of raw pointers to HashMemoryChunkData, the codebase achieves better type safety and code clarity. Functions that work with memory chunks can declare parameters and return values using HashMemoryChunk, making the intent and data flow more explicit in the hash join implementation.

This abstraction is particularly useful in the context of parallel hash joins, where memory chunks may be shared across multiple processes and need consistent typing for proper memory management and allocation routines.

## Parameters / Member Variables
This is a simple typedef with no direct members, but provides typed access to all HashMemoryChunkData members:
- Indirectly accesses , , , and  fields of the underlying HashMemoryChunkData structure
- Enables pointer arithmetic and linked list traversal operations with proper typing

## Dependencies
- Functions called/Symbols referenced:
  - HashMemoryChunkData (the underlying structure type)
- Called from (representative examples):
  - ExecHashIncreaseNumBatches (batch size management)
  - ExecParallelHashRepartitionFirst (parallel hash repartitioning)
  - ExecHashIncreaseNumBuckets (bucket resizing operations)
  - ExecParallelHashIncreaseNumBuckets (parallel bucket operations)
  - dense_alloc (memory allocation routines)
  - ExecParallelHashTupleAlloc (tuple allocation in parallel context)
  - ExecHashTableDetachBatch (batch cleanup operations)
  - ExecParallelHashTableSetCurrentBatch (batch switching)
  - ExecParallelHashPopChunkQueue (chunk queue management)
  - HashJoinTableData (as member types for chunk tracking)

## Notes and Other Information
- This typedef follows PostgreSQL's naming convention of providing clean interfaces for complex data structures
- Enables consistent typing across all hash join memory management functions
- Particularly important in parallel hash join scenarios where memory chunks are passed between different execution contexts
- Simplifies function signatures and improves code readability in the hash join implementation
- Used extensively in both serial and parallel hash join code paths for memory chunk manipulation
- Provides a stable API interface that abstracts the underlying HashMemoryChunkData structure changes