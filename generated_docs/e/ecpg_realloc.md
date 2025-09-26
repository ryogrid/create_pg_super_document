# ecpg_realloc

## Location
[src/interfaces/ecpg/ecpglib/memory.c:33-46](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/memory.c#L33-L46)

## Overview
Resizes previously allocated memory blocks with error handling and line number tracking for debugging purposes in the ECPG library.

## Definition

```c
char *
ecpg_realloc(void *ptr, long size, int lineno)
```
## Detailed Description
The  function provides a safe wrapper around the standard  function, offering memory reallocation with comprehensive error handling and debugging support. This function is essential for dynamically growing or shrinking memory blocks during ECPG operations, particularly when dealing with variable-length data or growing collections.

When reallocation fails, the function raises an ECPG error with the specific line number where the reallocation was attempted, enabling precise debugging. The original memory block remains unchanged if reallocation fails, following standard realloc() behavior.

The function handles all standard realloc() scenarios: expanding existing blocks, shrinking blocks, or moving data to new locations when necessary. It maintains the existing data in the memory block up to the minimum of the old and new sizes.

## Parameters / Member Variables
- `ptr`: Pointer to the previously allocated memory block to be resized (can be NULL, in which case this behaves like malloc)
- `size`: New size in bytes for the memory block
- `lineno`: Line number in the source code where the reallocation is requested, used for error reporting and debugging

## Dependencies
- Functions called/Symbols referenced:
  - realloc (standard C library function)
  - [ecpg_raise](ecpg_raise.md) (ECPG error reporting function)
  - ECPG_OUT_OF_MEMORY (error constant)
  - ECPG_SQLSTATE_ECPG_OUT_OF_MEMORY (SQL state constant)
- Called from (representative examples):
  - [var_list](../v/var_list.md) (variable list management)
  - [ecpg_store_input](ecpg_store_input.md) (parameter processing with dynamic sizing)
  - [ecpg_build_params](ecpg_build_params.md) (parameter array building)

## Notes and Other Information
- Returns NULL on reallocation failure after raising an appropriate error, leaving the original block unchanged
- If ptr is NULL, this function behaves identically to ecpg_alloc()
- Preserves existing data when expanding or contracting memory blocks
- The line number parameter enables precise error location tracking in complex ECPG applications
- Primarily used for dynamic memory management in parameter processing and variable-length data handling
- Less commonly used than ecpg_alloc() but critical for scenarios requiring memory block resizing
- Part of ECPG's comprehensive memory management system ensuring robust handling of dynamic data structures