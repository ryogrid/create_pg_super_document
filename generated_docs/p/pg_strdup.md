# pg_strdup

## Location
[src/common/fe_memutils.c:85-104](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/fe_memutils.c#L85-L104)

## Overview
A "safe" wrapper around the standard C library strdup() function that provides error checking and handling for null input pointers and memory allocation failures.

## Definition

```c
char *
pg_strdup(const char *in)
```
## Detailed Description
pg_strdup serves as a robust alternative to the standard strdup() function with built-in error handling. It duplicates a given string by allocating memory and copying the contents, but unlike the standard strdup(), it performs explicit null pointer checking and memory allocation failure detection. If either condition occurs, the function prints an appropriate error message to stderr and terminates the program with EXIT_FAILURE, ensuring that calling code doesn't need to handle these error conditions.

This function is part of PostgreSQL's frontend memory utilities, designed to provide safer memory operations for client-side tools and utilities that need reliable string duplication with automatic error handling.

## Parameters / Member Variables
- `*in`: The input string to be duplicated. Must not be NULL or the function will terminate the program with an error message.
## Dependencies
- Functions called/Symbols referenced:
  - strdup (standard C library function)
  - fprintf (for error reporting)
  - exit (program termination)
  - EXIT_FAILURE (exit status constant)
  - _ (internationalization macro for error messages)

- Called from (representative examples):
  - No direct references found in the current codebase analysis

## Notes and Other Information
- This function is designed for frontend utilities where program termination on memory errors is acceptable behavior
- The function uses internationalized error messages via the _() macro
- Unlike palloc family functions used in the backend, this function uses standard malloc-based allocation via strdup()
- The function is located in src/common/fe_memutils.c, indicating it's part of the frontend (client-side) memory utilities
- Error handling is aggressive - any failure results in immediate program termination rather than returning error codes