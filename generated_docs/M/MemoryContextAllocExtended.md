# MemoryContextAllocExtended

## Location
src/backend/utils/mmgr/mcxt.c: 1237 - 1270

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
  - MemoryContextAllocAligned
  - DynaHashAlloc
  - guc_malloc
  - guc_realloc

## Notes and Other Information
- Sets context->isReset to false to indicate the context has active allocations
- Returns NULL if the underlying allocation method fails
- Supports zero-initialization when MCXT_ALLOC_ZERO flag is set
- Validates allocation size limits differently for huge vs normal allocations
- Integrates with Valgrind memory debugging tools for leak detection