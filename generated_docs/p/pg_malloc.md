# pg_malloc

## Location
src/common/fe_memutils.c: 47 - 52

## Overview
Public interface function for memory allocation that provides PostgreSQL's standard malloc behavior with out-of-memory error handling.

## Definition

```c
void *
pg_malloc(size_t size)
```
## Detailed Description
pg_malloc is a wrapper function around pg_malloc_internal that provides the standard PostgreSQL memory allocation interface for frontend applications. It allocates the requested amount of memory and exits the program with an error message if allocation fails. This function is the PostgreSQL equivalent of the standard C library malloc() function but with added safety features including handling of malloc(0) edge cases and guaranteed program termination on out-of-memory conditions.

The function is widely used throughout PostgreSQL frontend utilities and tools as the primary memory allocation routine, providing consistent behavior across all PostgreSQL client applications.

## Parameters / Member Variables
- : The number of bytes to allocate

## Dependencies
- Functions called/Symbols referenced:
  - pg_malloc_internal (with flags = 0)
- Called from (representative examples):
  - escape_quotes_bki (initdb)
  - readfile (pg_ctl)
  - parallel_exec_prog (pg_upgrade)
  - parseVariable (pgbench)
  - strtokx (psql)
  - simple_string_list_append (fe_utils)

## Notes and Other Information
- This is the most commonly used memory allocation function in PostgreSQL frontend code
- Always terminates the program on allocation failure (no graceful error handling)
- Does not initialize allocated memory to zero - use pg_malloc0() for zero-initialized allocation
- Handles malloc(0) portability issues by ensuring at least 1 byte is allocated
- For applications that need to handle allocation failures gracefully, use pg_malloc_extended() with MCXT_ALLOC_NO_OOM flag
- Located in src/common/fe_memutils.c:47-52