# palloc_extended

## Location
[src/backend/utils/mmgr/mcxt.c:1367-1407](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/mcxt.c#L1367-L1407)

## Overview
Allocates memory from the current memory context with extended options, providing flags to control allocation behavior such as zero-initialization and handling of allocation failures.

## Definition

```c
void *
palloc_extended(Size size, int flags)
```
## Detailed Description
The `palloc_extended` function is the extended version of the basic `palloc` function that provides additional control over memory allocation behavior through flags. It allocates memory from the current memory context (`CurrentMemoryContext`) and allows the caller to specify various allocation options.

The function duplicates the functionality of `MemoryContextAllocExtended` to avoid increased overhead by working directly with the current memory context. It performs essential validation checks, marks the memory context as no longer reset, delegates the actual allocation to the context's allocation method, and optionally zeros the allocated memory if requested.

The function integrates with Valgrind memory debugging tools and handles allocation failure gracefully by returning NULL when requested through flags.

## Parameters / Member Variables
- `size`: The size in bytes of memory to allocate
- `flags`: Control flags for allocation behavior, including:
  - `MCXT_ALLOC_ZERO`: Initialize allocated memory to zero
  - Other flags for controlling failure handling behavior

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextIsValid
  - AssertNotInCriticalSection
  - VALGRIND_MEMPOOL_ALLOC
  - MCXT_ALLOC_ZERO
  - MemSetAligned
- Called from (representative examples):
  - [XLogReaderAllocate](../X/XLogReaderAllocate.md)
  - [BackendStartup](../B/BackendStartup.md)
  - [StartAutovacuumWorker](../S/StartAutovacuumWorker.md)
  - [assign_backendlist_entry](../a/assign_backendlist_entry.md)
  - pg_clean_ascii

## Notes and Other Information
- The function operates on the `CurrentMemoryContext` global variable
- Includes debug assertions to ensure the memory context is valid and not in a critical section
- Supports Valgrind integration for memory debugging
- Returns NULL on allocation failure instead of throwing an error when appropriate flags are set
- The zero-initialization using `MemSetAligned` is performed after allocation to ensure optimal performance
- Located in src/backend/utils/mmgr/mcxt.c at lines 1367-1407