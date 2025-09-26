# pg_malloc_internal

## Location
src/common/fe_memutils.c: 23 - 46

## Overview
Internal function that performs memory allocation with optional flags for zero-initialization and out-of-memory handling.

## Definition

```c
static inline void *
pg_malloc_internal(size_t size, int flags)
```
## Detailed Description
pg_malloc_internal is a static inline function that serves as the core implementation for PostgreSQL's frontend memory allocation routines. It wraps the standard C library malloc() function with additional safety features and PostgreSQL-specific behavior. The function handles the unportable behavior of malloc(0) by ensuring at least 1 byte is allocated, provides optional zero-initialization of allocated memory, and offers configurable out-of-memory error handling.

The function is designed to be used internally by other PostgreSQL memory allocation functions like pg_malloc, pg_malloc0, and pg_malloc_extended, providing a consistent base implementation with flexible behavior controlled through flags.

## Parameters / Member Variables
- : The number of bytes to allocate. If 0, the function will allocate 1 byte to avoid unportable malloc(0) behavior
- : Control flags that modify allocation behavior:
  - : If set, returns NULL on allocation failure instead of exiting
  - : If set, initializes the allocated memory to zero

## Dependencies
- Functions called/Symbols referenced:
  - malloc (standard C library function)
  - fprintf (for error reporting)
  - exit (with EXIT_FAILURE on OOM)
  - MemSet (PostgreSQL macro for memory initialization)
- Called from (representative examples):
  - pg_malloc
  - pg_malloc0
  - pg_malloc_extended
  - palloc
  - palloc0
  - palloc_extended

## Notes and Other Information
- This is a static inline function, meaning it's internal to the fe_memutils.c compilation unit and will be inlined at call sites
- The function ensures malloc(0) returns a valid pointer by allocating at least 1 byte
- Default behavior on out-of-memory is to print an error message and exit the program
- The MCXT_ALLOC_NO_OOM flag allows callers to handle allocation failures gracefully
- Zero-initialization is performed using PostgreSQL's MemSet macro rather than calloc() to maintain consistent behavior
- Located in src/common/fe_memutils.c:23-46