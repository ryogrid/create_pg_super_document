# MemoryContextAllocExtended

## Location
[src/backend/utils/mmgr/mcxt.c:1237-1270](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/mcxt.c#L1237-L1270)

## Overview
MemoryContextAllocExtended is the core memory allocation function in PostgreSQL's memory management system that allocates space within a specified memory context using configurable flags for allocation behavior.

## Definition
```c
void *MemoryContextAllocExtended(MemoryContext context, Size size, int flags)
```

## Detailed Description
This function serves as the fundamental memory allocation routine in PostgreSQL's memory context system. It provides extended allocation capabilities through flags that control allocation behavior, such as whether to zero-initialize memory or allow huge allocations. The function validates the memory context, checks allocation size limits, delegates to the context-specific allocation method, and applies post-allocation processing based on the provided flags.

## Parameters / Member Variables
- `context`: The memory context within which to allocate memory
- `size`: The number of bytes to allocate
- `flags`: Bit flags controlling allocation behavior (MCXT_ALLOC_HUGE, MCXT_ALLOC_ZERO, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextIsValid
  - AssertNotInCriticalSection
  - AllocHugeSizeIsValid
  - AllocSizeIsValid
  - VALGRIND_MEMPOOL_ALLOC
  - MemSetAligned
- Called from (representative examples):
  - [MemoryContextAllocAligned](MemoryContextAllocAligned.md)
  - [DynaHashAlloc](../D/DynaHashAlloc.md)
  - [guc_malloc](../g/guc_malloc.md)
  - [guc_realloc](../g/guc_realloc.md)

## Notes and Other Information
- Sets context->isReset to false to indicate the context has active allocations
- Returns NULL if the underlying allocation method fails
- Supports zero-initialization when MCXT_ALLOC_ZERO flag is set
- Validates allocation size limits differently for huge vs normal allocations
- Integrates with Valgrind memory debugging tools for leak detection

## Simplified Source

```c
// Simplified version of MemoryContextAllocExtended
void *MemoryContextAllocExtended(MemoryContext context, Size size, int flags) {
    void *ret;

    // Validate inputs: context and allocation size
    Assert(MemoryContextIsValid(context));
    if (!AllocSizeIsValid(size)) {
        elog(ERROR, "invalid memory alloc request size %zu", size);
    }

    // Mark context as having active allocations
    context->isReset = false;

    // Delegate to context-specific allocation method
    ret = context->methods->alloc(context, size, flags);
    if (ret == NULL) {
        return NULL;
    }

    // Zero-initialize memory if requested
    if (flags & MCXT_ALLOC_ZERO) {
        MemSetAligned(ret, 0, size);
    }

    return ret;
}
```

Key simplifications made:
- Removed AssertNotInCriticalSection check for clarity
- Simplified size validation by removing huge allocation special case
- Removed Valgrind integration calls
- Consolidated flag checking logic
- Focused on the main allocation workflow