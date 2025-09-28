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

## Simplified Source

```c
// Simplified version of palloc
void *palloc(Size size) {
    MemoryContext context = CurrentMemoryContext;

    // Validate context and ensure we're not in a critical section
    Assert(MemoryContextIsValid(context));
    AssertNotInCriticalSection(context);

    // Mark context as having active allocations
    context->isReset = false;

    // Delegate to context-specific allocation method
    void *ret = context->methods->alloc(context, size, 0);

    // OOM is handled by alloc function, so ret should never be NULL
    Assert(ret != NULL);

    // Track allocation for debugging tools
    VALGRIND_MEMPOOL_ALLOC(context, ret, size);

    return ret;
}
```

Key simplifications made:
- Added clear comments explaining each major step
- Consolidated variable declarations at the top
- Removed the detailed performance comments for clarity
- Focused on the core algorithm: validate context, mark as active, allocate, track, return
- Preserved all assertions and debugging support