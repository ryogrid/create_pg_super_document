# pgtypes_alloc

## Location
[src/interfaces/ecpg/pgtypeslib/common.c:10-19](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/common.c#L10-L19)

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
  - [pgtypes_fmt_replace](pgtypes_fmt_replace.md)
  - [PGTYPESdate_new](../P/PGTYPESdate_new.md)
  - [PGTYPESdate_fmt_asc](../P/PGTYPESdate_fmt_asc.md)
  - `PGTYPESdate_defmt_asc`
  - [PGTYPEStimestamp_defmt_scan](../P/PGTYPEStimestamp_defmt_scan.md)
  - [PGTYPESinterval_new](../P/PGTYPESinterval_new.md)
  - [PGTYPESinterval_from_asc](../P/PGTYPESinterval_from_asc.md)
  - `digitbuf_alloc`
  - [PGTYPESnumeric_new](../P/PGTYPESnumeric_new.md)
  - [PGTYPESdecimal_new](../P/PGTYPESdecimal_new.md)
  - [get_str_from_var](../g/get_str_from_var.md)
  - [PGTYPESnumeric_from_asc](../P/PGTYPESnumeric_from_asc.md)
  - `un_fmt_comb`

## Notes and Other Information
- Returns a pointer to zero-initialized memory on success, or NULL on failure
- Sets `errno` to `ENOMEM` when allocation fails, providing consistent error reporting
- Used extensively throughout the pgtypes library for allocating memory for various PostgreSQL data type representations
- The zero-initialization is crucial for proper initialization of complex data structures used by PostgreSQL data types
- Located in `src/interfaces/ecpg/pgtypeslib/common.c:10-19`

## Simplified Source

```c
char *pgtypes_alloc(long size) {
    // Allocate zero-initialized memory
    char *new = (char *) calloc(1L, size);

    // Set errno if allocation failed
    if (!new) {
        errno = ENOMEM;
    }

    return new;
}
```