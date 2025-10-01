# memory_exhausted

## Location
[src/timezone/zic.c:411-417](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L411-L417)

## Overview
A static error handling function in the timezone zic utility that handles memory exhaustion scenarios by printing an error message and terminating the program.

## Definition
```c
static void memory_exhausted(const char *msg)
```

## Detailed Description
The `memory_exhausted` function serves as a centralized error handler for memory allocation failures within the zic (Zone Information Compiler) utility. When called, it prints a localized error message to stderr indicating memory exhaustion along with the specific context provided in the `msg` parameter, then terminates the program with an exit status indicating failure. This function follows a fail-fast approach, ensuring that the program does not continue execution when memory allocation fails, which could lead to unpredictable behavior or data corruption.

## Parameters / Member Variables
- `msg`: A string describing the context or operation that failed due to memory exhaustion

## Dependencies
- Functions called/Symbols referenced:
  - fprintf (standard library function for formatted output)
  - exit (standard library function for program termination)
  - EXIT_FAILURE (standard exit code constant for failure)
  - _ (gettext macro for internationalization)
  - progname (global variable containing program name)
- Called from (representative examples):
  - [size_product](../s/size_product.md)
  - [memcheck](memcheck.md)
  - [growalloc](../g/growalloc.md)

## Notes and Other Information
- This is a static function, meaning it is only accessible within the src/timezone/zic.c file
- The function uses gettext internationalization (_) to provide localized error messages
- It represents a defensive programming practice by providing a single point of failure handling for memory allocation errors
- The function never returns, as it always calls exit() with EXIT_FAILURE status

## Simplified Source

```c
static void memory_exhausted(const char *msg) {
    // Print error message with program name and context
    fprintf(stderr, _("%s: Memory exhausted: %s\n"), progname, msg);

    // Terminate program with failure status
    exit(EXIT_FAILURE);
}
```