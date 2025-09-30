# pg_realloc

## Location
[src/common/fe_memutils.c:65-84](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/fe_memutils.c#L65-L84)

## Overview
Memory reallocation function that resizes an existing memory block with PostgreSQL's standard error handling.

## Definition

```c
void *
pg_realloc(void *ptr, size_t size)
```
## Detailed Description
pg_realloc is PostgreSQL's frontend wrapper around the standard C library realloc() function. It provides memory block resizing capabilities with consistent error handling across PostgreSQL applications. The function handles the unportable behavior of realloc(NULL, 0) by ensuring at least 1 byte is allocated in this edge case, and exits the program with an error message if reallocation fails.

This function is used throughout PostgreSQL frontend utilities when dynamic data structures need to be resized, such as growing arrays, expanding buffers, or adjusting string storage.

## Parameters / Member Variables
- : Pointer to the previously allocated memory block to resize, or NULL to allocate new memory
- : The new size in bytes for the memory block

## Dependencies
- Functions called/Symbols referenced:
  - realloc (standard C library function)
  - fprintf (for error reporting)
  - exit (with EXIT_FAILURE on reallocation failure)
- Called from (representative examples):
  - [readfile](../r/readfile.md) (initdb, pgbench)
  - [extend_pattern_info_array](../e/extend_pattern_info_array.md) (pg_amcheck)
  - [readMessageFromPipe](../r/readMessageFromPipe.md) (pg_dump parallel)
  - [datapagemap_add](../d/datapagemap_add.md) (pg_rewind)
  - [enlargeVariables](../e/enlargeVariables.md) (pgbench)
  - [exec_command_set](../e/exec_command_set.md) (psql)
  - [repalloc](../r/repalloc.md) (fe_memutils wrapper)

## Notes and Other Information
- Handles the edge case of realloc(NULL, 0) by allocating at least 1 byte to avoid unportable behavior
- If ptr is NULL, behaves like pg_malloc(size)
- If size is 0 and ptr is not NULL, the behavior follows standard realloc() semantics (may free the memory)
- Always terminates the program on reallocation failure (no graceful error handling available)
- The original memory contents are preserved up to the minimum of the old and new sizes
- Unlike the pg_malloc family, there is no extended version with configurable flags
- Located in src/common/fe_memutils.c:65-84

## Simplified Source

```c
void *pg_realloc(void *ptr, size_t size) {
    // Handle edge case: avoid unportable realloc(NULL, 0)
    if (ptr == NULL && size == 0) {
        size = 1;
    }

    // Attempt reallocation
    void *tmp = realloc(ptr, size);

    // Exit program if reallocation fails
    if (!tmp) {
        fprintf(stderr, _("out of memory\n"));
        exit(EXIT_FAILURE);
    }

    return tmp;
}
```