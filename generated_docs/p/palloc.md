# palloc

## Location
[src/backend/utils/mmgr/mcxt.c:1316-1345](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/mcxt.c#L1316-L1345)

## Overview
palloc is PostgreSQL's primary memory allocation function that allocates memory from the current memory context, providing a simplified interface to the memory context system.

## Definition
```c
void *palloc(Size size)
```

## Detailed Description
This function serves as the most commonly used memory allocation interface in PostgreSQL. It allocates memory from the CurrentMemoryContext and is designed for maximum efficiency by duplicating some logic from MemoryContextAlloc to avoid function call overhead. The function is optimized for performance with sibling call optimization and delegates error handling to the underlying memory context methods. It expects that out-of-memory conditions are handled by the allocation function itself, making the common case as fast as possible.

## Parameters / Member Variables
- `size`: The number of bytes to allocate

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextIsValid
  - AssertNotInCriticalSection
  - VALGRIND_MEMPOOL_ALLOC
  - CurrentMemoryContext (global variable)
- Called from (representative examples):
  - (This function is widely used throughout PostgreSQL but specific references weren't found in the current analysis)

## Notes and Other Information
- Optimized for performance by avoiding extra function call overhead
- Uses CurrentMemoryContext as the allocation context
- Delegates failure handling to the underlying alloc method for efficiency
- Sets context->isReset to false to mark the context as having active allocations
- Designed to enable compiler optimizations through sibling call optimization
- Integrates with Valgrind for memory debugging
- Expects the underlying allocation method to handle OOM conditions and never return NULL