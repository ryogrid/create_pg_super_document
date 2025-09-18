# palloc0

## Location
[src/backend/utils/mmgr/mcxt.c:1346-1366](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/mcxt.c#L1346-L1366)

## Overview
palloc0 is PostgreSQL's zero-initialized memory allocation function that allocates memory from the current memory context and automatically clears it to zero, providing a convenient interface for allocating clean memory.

## Definition
```c
void *palloc0(Size size)
```

## Detailed Description
This function is a variant of palloc that provides zero-initialized memory allocation. It allocates memory from the CurrentMemoryContext and then clears the allocated memory to zero using MemSetAligned. Like palloc, it's optimized for performance by duplicating logic from MemoryContextAllocZero to avoid function call overhead. The function is commonly used when clean, initialized memory is required, eliminating the need for separate allocation and memset operations.

## Parameters / Member Variables
- `size`: The number of bytes to allocate

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextIsValid
  - AssertNotInCriticalSection
  - VALGRIND_MEMPOOL_ALLOC
  - MemSetAligned
  - CurrentMemoryContext (global variable)
- Called from (representative examples):
  - (This function is widely used throughout PostgreSQL but specific references weren't found in the current analysis)

## Notes and Other Information
- Optimized for performance by avoiding extra function call overhead
- Uses CurrentMemoryContext as the allocation context
- Automatically zero-initializes the allocated memory using MemSetAligned
- Sets context->isReset to false to mark the context as having active allocations
- Integrates with Valgrind for memory debugging
- More efficient than separate palloc() + memset() calls
- Commonly used for allocating structures that need to start with clean state