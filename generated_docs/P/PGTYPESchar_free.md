# PGTYPESchar_free

## Location
src/interfaces/ecpg/pgtypeslib/common.c: 145 - 148

## Overview
A simple memory deallocation wrapper function in the PostgreSQL ECPG pgtypeslib that provides a platform-independent interface for freeing memory allocated by pgtypes functions.

## Definition
```c
void PGTYPESchar_free(char *ptr)
```

## Detailed Description
`PGTYPESchar_free` is a straightforward wrapper function around the standard C library `free()` function. Its primary purpose is to provide a consistent and platform-independent memory deallocation interface for the PostgreSQL ECPG pgtypes library. The function is particularly important on Windows platforms where memory allocated by one library component must be freed by the same component to avoid heap corruption issues.

The function serves as the counterpart to memory allocation functions within the pgtypes library (such as those that use `pgtypes_alloc` or `pgtypes_strdup`), ensuring that memory management remains consistent throughout the library ecosystem. By providing this wrapper, the pgtypes library maintains control over memory management and can ensure compatibility across different platforms and compiler environments.

## Parameters / Member Variables
- `ptr`: A pointer to the memory block to be freed, typically allocated by pgtypes library functions

## Dependencies
- Functions called/Symbols referenced:
  - `free` (standard C library function for memory deallocation)
- Called from (representative examples):
  - Extensively used in test files:
    - `pgtypeslib-dt_test.c` (multiple locations for testing date/time functionality)
    - `pgtypeslib-dt_test2.c` (for date/time testing)
    - `pgtypeslib-num_test.c` (for numeric type testing)
    - `pgtypeslib-num_test2.c` (for additional numeric testing)
    - `sql-sqlda.c` (in `dump_sqlda` function)
  - Declared in header file:
    - `pgtypes.h` (public interface declaration)

## Notes and Other Information
- This function is part of the public PGTYPES API as declared in `pgtypes.h`
- Primarily needed for Windows compatibility where memory allocated by one DLL component must be freed by the same component
- Should be used to free any memory returned by pgtypes library functions that return dynamically allocated strings
- The function performs no validation on the input pointer - it directly calls the standard `free()` function
- Essential for proper memory management when using PostgreSQL ECPG pgtypes library functions
- Located in `src/interfaces/ecpg/pgtypeslib/common.c:145-148`