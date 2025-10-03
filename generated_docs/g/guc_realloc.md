# guc_realloc

## Location
[src/backend/utils/misc/guc.c:654-678](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L654-L678)

## Overview
GUC-related memory reallocation function that resizes previously allocated memory in the GUC memory context with configurable error reporting level.

## Definition

```c
void *
guc_realloc(int elevel, void *old, size_t size)
```
## Detailed Description
 is a PostgreSQL-specific memory reallocation function designed for the GUC (Grand Unified Configuration) system. It provides functionality similar to the standard C library's  but operates within PostgreSQL's memory context system, specifically the . The function handles both resizing existing allocations and allocating new memory when the old pointer is NULL.

The function includes an important safety feature: it verifies that any existing memory being reallocated actually belongs to the GUCMemoryContext using an assertion. This helps catch programming errors where GUC functions might be called on memory allocated elsewhere. Like the standard , it allows the old pointer to be NULL, in which case it behaves like a fresh allocation.

## Parameters / Member Variables
- `elevel`: Error level to use when reporting out-of-memory conditions (e.g., ERROR, WARNING, LOG)
- `*old`: Pointer to previously allocated memory block, or NULL for new allocation
- `size`: New size in bytes for the memory block
## Dependencies
- Functions called/Symbols referenced:
  - [GetMemoryChunkContext](../G/GetMemoryChunkContext.md) (for memory context verification)
  - [repalloc_extended](../r/repalloc_extended.md) (for resizing existing memory)
  - [MemoryContextAllocExtended](../M/MemoryContextAllocExtended.md) (for new allocations when old is NULL)
  - MCXT_ALLOC_NO_OOM (flag for no-OOM allocation)
  - ereport (for error reporting)
  - [errcode](../e/errcode.md), errmsg (for error details)

- Called from (representative examples):
  - [read_string_with_null](../r/read_string_with_null.md)
  - EmitWarningsOnPlaceholders

## Notes and Other Information
- Combines reallocation and allocation functionality similar to standard 
- Includes safety assertion to verify memory belongs to GUCMemoryContext
- Uses extended allocation functions with no-OOM flags for controlled error handling
- Returns NULL and reports error at specified level if allocation fails
- Part of the GUC infrastructure for managing configuration-related memory
- Handles both the case where old memory exists (reallocation) and where it doesn't (new allocation)
- The assertion helps catch bugs where non-GUC memory is passed to GUC memory functions