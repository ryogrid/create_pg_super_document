# erealloc

## Location
[src/timezone/zic.c:440-445](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L440-L445)

## Overview
A static wrapper function around realloc() that provides automatic error checking and program termination on reallocation failure.

## Definition
```c
static void *erealloc(void *ptr, size_t size)
```

## Detailed Description
The `erealloc` function is a safe wrapper around the standard library `realloc()` function that automatically handles reallocation failures. It calls `realloc()` to resize or relocate a previously allocated memory block to the new requested size, then immediately passes the result through `memcheck()` to verify the reallocation succeeded. If realloc() returns NULL (indicating reallocation failure), memcheck() will call `memory_exhausted()` to print an error message and terminate the program. If the reallocation succeeds, the function returns the pointer to the resized memory block. This design ensures that all memory reallocations in the zic utility are checked for failure, maintaining the fail-fast approach that prevents the program from continuing with invalid pointers.

## Parameters / Member Variables
- `ptr`: Pointer to the previously allocated memory block to be resized, or NULL for new allocation
- `size`: The new size in bytes for the memory block

## Dependencies
- Functions called/Symbols referenced:
  - realloc (standard library memory reallocation function)
  - memcheck (validation function for allocation results)
- Called from (representative examples):
  - growalloc

## Notes and Other Information
- This is a static function, accessible only within the src/timezone/zic.c file
- Provides a consistent interface for memory reallocation throughout the zic utility
- Eliminates the need for manual NULL pointer checks after each realloc() call
- Part of a comprehensive memory management strategy that includes emalloc for initial allocation
- The function either returns a valid pointer or terminates the program - never returns NULL
- Follows the common naming convention of prefixing error-checking wrappers with "e"
- If ptr is NULL, behaves like malloc(size); if size is 0, behavior is implementation-defined (typically frees the memory)