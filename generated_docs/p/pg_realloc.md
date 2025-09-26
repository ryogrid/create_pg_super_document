# pg_realloc

## Location
src/common/fe_memutils.c: 65 - 84

## Overview
Memory reallocation function that resizes an existing memory block with PostgreSQL's standard error handling.

## Definition


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
  - readfile (initdb, pgbench)
  - extend_pattern_info_array (pg_amcheck)
  - readMessageFromPipe (pg_dump parallel)
  - datapagemap_add (pg_rewind)
  - enlargeVariables (pgbench)
  - exec_command_set (psql)
  - repalloc (fe_memutils wrapper)

## Notes and Other Information
- Handles the edge case of realloc(NULL, 0) by allocating at least 1 byte to avoid unportable behavior
- If ptr is NULL, behaves like pg_malloc(size)
- If size is 0 and ptr is not NULL, the behavior follows standard realloc() semantics (may free the memory)
- Always terminates the program on reallocation failure (no graceful error handling available)
- The original memory contents are preserved up to the minimum of the old and new sizes
- Unlike the pg_malloc family, there is no extended version with configurable flags
- Located in src/common/fe_memutils.c:65-84