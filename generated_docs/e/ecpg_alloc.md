# ecpg_alloc

## Location
[src/interfaces/ecpg/ecpglib/memory.c:19-32](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/memory.c#L19-L32)

## Overview
Allocates zero-initialized memory with error handling and line number tracking for debugging purposes in the ECPG library.

## Definition

```c
char *
ecpg_alloc(long size, int lineno)
```
## Detailed Description
The  function is ECPG's primary memory allocation routine that provides a safe wrapper around the standard  function. Unlike simple malloc wrappers, this function ensures that allocated memory is zero-initialized and includes comprehensive error handling with diagnostic information.

When memory allocation fails, the function raises an ECPG error with the specific line number where the allocation was attempted, making debugging much easier. This is particularly valuable in embedded SQL applications where memory allocation failures can lead to complex error scenarios.

The function allocates memory using , ensuring that all allocated memory is initialized to zero, which helps prevent bugs related to uninitialized memory usage.

## Parameters / Member Variables
- : The number of bytes to allocate
- : Line number in the source code where the allocation is requested, used for error reporting and debugging

## Dependencies
- Functions called/Symbols referenced:
  - calloc (standard C library function)
  - [ecpg_raise](ecpg_raise.md) (ECPG error reporting function)
  - ECPG_OUT_OF_MEMORY (error constant)
  - ECPG_SQLSTATE_ECPG_OUT_OF_MEMORY (SQL state constant)
- Called from (representative examples):
  - [ECPGconnect](../E/ECPGconnect.md) (connection establishment)
  - [ecpg_get_data](ecpg_get_data.md) (data retrieval operations)
  - [ECPGset_desc](../E/ECPGset_desc.md) (descriptor operations)
  - [ecpg_store_input](ecpg_store_input.md) (parameter processing)
  - [ecpg_build_params](ecpg_build_params.md) (parameter building)
  - [ecpg_auto_alloc](ecpg_auto_alloc.md) (automatic memory management)
  - [replace_variables](../r/replace_variables.md) (SQL statement processing)

## Notes and Other Information
- Returns NULL on allocation failure after raising an appropriate error
- All allocated memory is zero-initialized via calloc(), making it safer than malloc-based alternatives
- The line number parameter enables precise error location tracking in complex ECPG applications
- Extensively used throughout ECPG for connection management, descriptor handling, statement processing, and data conversion
- Part of ECPG's comprehensive memory management system that helps ensure robust embedded SQL applications
- The zero-initialization feature helps prevent common bugs related to uninitialized pointers and data structures

## Simplified Source

```c
char *
ecpg_alloc(long size, int lineno)
{
    // Allocate zero-initialized memory
    char *new = (char *) calloc(1L, size);

    // Handle allocation failure
    if (!new) {
        ecpg_raise(lineno, ECPG_OUT_OF_MEMORY, ECPG_SQLSTATE_ECPG_OUT_OF_MEMORY, NULL);
        return NULL;
    }

    return new;
}
```