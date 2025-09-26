# emalloc

## Location
[src/timezone/zic.c:434-439](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L434-L439)

## Overview
A static wrapper function around malloc() that provides automatic error checking and program termination on allocation failure.

## Definition
```c
static void *emalloc(size_t size)
```

## Detailed Description
The `emalloc` function is a safe wrapper around the standard library `malloc()` function that automatically handles allocation failures. It calls `malloc()` to allocate the requested amount of memory, then immediately passes the result through `memcheck()` to verify the allocation succeeded. If malloc() returns NULL (indicating allocation failure), memcheck() will call `memory_exhausted()` to print an error message and terminate the program. If the allocation succeeds, the function returns the allocated memory pointer. This design ensures that all memory allocations in the zic utility are checked for failure, following a fail-fast approach that prevents the program from continuing with invalid pointers.

## Parameters / Member Variables
- `size`: The number of bytes to allocate

## Dependencies
- Functions called/Symbols referenced:
  - malloc (standard library memory allocation function)
  - memcheck (validation function for allocation results)
- Called from (representative examples):
  - relname
  - itsdir
  - writezone
  - outzone
  - getfields

## Notes and Other Information
- This is a static function, accessible only within the src/timezone/zic.c file
- Provides a consistent interface for memory allocation throughout the zic utility
- Eliminates the need for manual NULL pointer checks after each malloc() call
- Part of a comprehensive memory management strategy that includes size_product for overflow checking
- The function either returns a valid pointer or terminates the program - never returns NULL
- Follows the common naming convention of prefixing error-checking wrappers with "e"