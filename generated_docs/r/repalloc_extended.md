# repalloc_extended

## Location
[src/backend/utils/mmgr/mcxt.c:1581-1617](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/mcxt.c#L1581-L1617)

## Overview
repalloc_extended is a PostgreSQL memory management function that adjusts the size of a previously allocated memory chunk, with support for HUGE and NO_OOM allocation flags.

## Definition
```c
void *repalloc_extended(void *pointer, Size size, int flags)
```

## Detailed Description
This function serves as an extended version of the standard realloc operation within PostgreSQL's memory context system. It provides additional control through flags for handling large allocations (HUGE) and out-of-memory conditions (NO_OOM). The function delegates the actual reallocation work to the memory context's specific realloc method implementation, allowing for different memory management strategies across various context types.

The function includes optimization considerations, specifically designed to leverage compiler sibling call optimization by avoiding post-call instructions. It also integrates with debugging tools through Valgrind memory pool tracking.

## Parameters / Member Variables
- `pointer`: Pointer to the previously allocated memory chunk to be resized
- `size`: New desired size for the memory chunk
- `flags`: Control flags that can include MCXT_ALLOC_HUGE for large allocations and MCXT_ALLOC_NO_OOM for non-failing behavior

## Dependencies
- Functions called/Symbols referenced:
  - [GetMemoryChunkContext](../G/GetMemoryChunkContext.md) (retrieves the memory context associated with a chunk)
  - AssertNotInCriticalSection (ensures not in critical section during allocation)
  - MCXT_METHOD (macro for calling memory context method implementations)
  - VALGRIND_MEMPOOL_CHANGE (Valgrind integration for memory pool tracking)
- Called from (representative examples):
  - [guc_realloc](../g/guc_realloc.md) (GUC configuration value reallocation)
  - [repalloc_huge](repalloc_huge.md) (huge memory reallocation wrapper)
  - REALLOC (regex library reallocation macro)

## Notes and Other Information
- The function includes assertions to verify the memory context is not in reset state
- Designed for compiler optimization through sibling call patterns
- Integrates with PostgreSQL's memory context debugging and profiling infrastructure
- Handles allocation failures by returning NULL when the underlying realloc method fails
- Located in src/backend/utils/mmgr/mcxt.c:1581-1617