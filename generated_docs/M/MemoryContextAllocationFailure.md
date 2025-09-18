# MemoryContextAllocationFailure

## Location
[src/backend/utils/mmgr/mcxt.c:1147-1166](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/mcxt.c#L1147-L1166)

## Overview
MemoryContextAllocationFailure handles memory allocation failures in PostgreSQL memory contexts, providing consistent behavior based on allocation flags when malloc returns NULL.

## Definition
```c
void *MemoryContextAllocationFailure(MemoryContext context, Size size, int flags)
```

## Detailed Description
This function serves as a centralized handler for memory allocation failures across all memory context implementations. It provides a standardized response when underlying malloc operations fail, with behavior determined by the MCXT_ALLOC_NO_OOM flag.

When allocation fails:
- If MCXT_ALLOC_NO_OOM is NOT set in flags: The function prints memory context statistics for debugging, then raises an ERROR with detailed information about the failed allocation
- If MCXT_ALLOC_NO_OOM IS set in flags: The function simply returns NULL, allowing the caller to handle the failure gracefully

This design allows PostgreSQL to support both error-on-failure (default) and return-NULL-on-failure allocation patterns depending on the calling context's needs.

## Parameters / Member Variables
- `context`: The memory context in which the allocation failure occurred
- `size`: The requested allocation size that failed 
- `flags`: Allocation flags controlling failure behavior, particularly MCXT_ALLOC_NO_OOM

## Dependencies
- Functions called/Symbols referenced:
  - MCXT_ALLOC_NO_OOM (flag constant for controlling failure behavior)
  - [MemoryContextStats](MemoryContextStats.md) (for printing debugging information)
  - ereport/errcode/errmsg/errdetail (for error reporting)
- Called from (representative examples):
  - [AlignedAllocRealloc](../A/AlignedAllocRealloc.md)
  - [AllocSetAllocLarge](../A/AllocSetAllocLarge.md)  
  - [AllocSetAllocFromNewBlock](../A/AllocSetAllocFromNewBlock.md)
  - [BumpAllocLarge](../B/BumpAllocLarge.md)
  - [GenerationAllocLarge](../G/GenerationAllocLarge.md)
  - [SlabAllocFromNewBlock](../S/SlabAllocFromNewBlock.md)

## Notes and Other Information
- This function is intended for use only by MemoryContextMethods implementations, not general application code
- The function always returns NULL, but may not return at all if it raises an ERROR
- When raising errors, it includes helpful context like the requested size and context name for debugging
- [MemoryContextStats](MemoryContextStats.md) output helps diagnose memory usage patterns leading to allocation failures
- The MCXT_ALLOC_NO_OOM flag enables 'try-allocate' semantics similar to C++'s nothrow new operator