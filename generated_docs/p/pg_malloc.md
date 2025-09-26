# pg_malloc

## Location
[src/common/fe_memutils.c:47-52](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/fe_memutils.c#L47-L52)

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
  - [pg_malloc_internal](pg_malloc_internal.md) (with flags = 0)
- Called from (representative examples):
  - [escape_quotes_bki](../e/escape_quotes_bki.md) (initdb)
  - [readfile](../r/readfile.md) (pg_ctl)
  - [parallel_exec_prog](parallel_exec_prog.md) (pg_upgrade)
  - [parseVariable](parseVariable.md) (pgbench)
  - [strtokx](../s/strtokx.md) (psql)
  - [simple_string_list_append](../s/simple_string_list_append.md) (fe_utils)

## Notes and Other Information
- This is the most commonly used memory allocation function in PostgreSQL frontend code
- Always terminates the program on allocation failure (no graceful error handling)
- Does not initialize allocated memory to zero - use pg_malloc0() for zero-initialized allocation
- Handles malloc(0) portability issues by ensuring at least 1 byte is allocated
- For applications that need to handle allocation failures gracefully, use pg_malloc_extended() with MCXT_ALLOC_NO_OOM flag
- Located in src/common/fe_memutils.c:47-52