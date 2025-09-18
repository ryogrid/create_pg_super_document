# pgtypes_alloc

## Location
src/interfaces/ecpg/pgtypeslib/common.c: 10 - 19

## Overview
A memory allocation wrapper function in the PostgreSQL ECPG pgtypeslib that allocates zero-filled memory blocks and sets errno on allocation failure.

## Definition
```c
char *pgtypes_alloc(long size)
```

## Detailed Description
`pgtypes_alloc` is a utility function that provides a consistent memory allocation interface for the PostgreSQL ECPG pgtypes library. It wraps the standard `calloc` function to allocate zero-initialized memory blocks. The function includes error handling by setting the global `errno` variable to `ENOMEM` when memory allocation fails, providing a standardized way to handle allocation failures throughout the pgtypes library.

The function is designed to return zero-filled memory, which is important for initializing data structures in a predictable state. This is particularly useful for PostgreSQL data types that need to be properly initialized before use.

## Parameters / Member Variables
- `size`: The number of bytes to allocate

## Dependencies
- Functions called/Symbols referenced:
  - `calloc` (standard C library function for zero-initialized allocation)
- Called from (representative examples):
  - `pgtypes_fmt_replace`
  - `PGTYPESdate_new`
  - `PGTYPESdate_fmt_asc`
  - `PGTYPESdate_defmt_asc`
  - `PGTYPEStimestamp_defmt_scan`
  - `PGTYPESinterval_new`
  - `PGTYPESinterval_from_asc`
  - `digitbuf_alloc`
  - `PGTYPESnumeric_new`
  - `PGTYPESdecimal_new`
  - `get_str_from_var`
  - `PGTYPESnumeric_from_asc`
  - `un_fmt_comb`

## Notes and Other Information
- Returns a pointer to zero-initialized memory on success, or NULL on failure
- Sets `errno` to `ENOMEM` when allocation fails, providing consistent error reporting
- Used extensively throughout the pgtypes library for allocating memory for various PostgreSQL data type representations
- The zero-initialization is crucial for proper initialization of complex data structures used by PostgreSQL data types
- Located in `src/interfaces/ecpg/pgtypeslib/common.c:10-19`