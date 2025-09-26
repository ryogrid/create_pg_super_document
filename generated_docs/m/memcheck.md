# memcheck

## Location
src/timezone/zic.c: 426 - 433

## Overview
A static utility function that validates the result of memory allocation operations and handles allocation failures by terminating the program with an appropriate error message.

## Definition
```c
static void *memcheck(void *ptr)
```

## Detailed Description
The `memcheck` function serves as a wrapper for validating memory allocation results in the zic utility. It takes a pointer returned from a memory allocation function (malloc, realloc, etc.) and checks if the allocation was successful. If the pointer is NULL (indicating allocation failure), it calls `memory_exhausted` with the system error message obtained from `strerror(errno)` to provide detailed information about the failure reason, then terminates the program. If the allocation was successful, it simply returns the pointer unchanged. This function implements a fail-fast approach to memory allocation errors, ensuring consistent error handling across all memory allocation operations in the zic utility.

## Parameters / Member Variables
- `ptr`: Pointer returned from a memory allocation function to be validated

## Dependencies
- Functions called/Symbols referenced:
  - memory_exhausted (for handling allocation failure)
  - strerror (standard library function to convert errno to error string)
  - errno (global error number variable)
- Called from (representative examples):
  - emalloc
  - erealloc  
  - ecpyalloc

## Notes and Other Information
- This is a static function, accessible only within the src/timezone/zic.c file
- Implements a common defensive programming pattern for robust memory allocation error handling
- Provides detailed error information by using strerror(errno) to convert system error codes to human-readable messages
- Acts as a centralized validation point for all memory allocation operations
- The function either returns a valid pointer or terminates the program - it never returns NULL