# pg_vsprintf

## Location
[src/port/snprintf.c:214-229](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/snprintf.c#L214-L229)

## Overview
pg_vsprintf is PostgreSQL's portable implementation of vsprintf that formats a string into an unbounded buffer using a va_list argument.

## Definition

```c
int
pg_vsprintf(char *str, const char *fmt, va_list args)
```
## Detailed Description
pg_vsprintf provides a portable alternative to the standard vsprintf function. Unlike pg_vsnprintf which takes a buffer size limit, pg_vsprintf assumes the output buffer is large enough to hold the entire formatted string. It formats the format string `fmt` with the variable arguments contained in `args` and stores the result in the buffer `str`. The function sets the PrintfTarget's bufend to NULL to indicate unlimited buffer mode, allowing dopr() to write without bounds checking. This function should only be used when you can guarantee the buffer is sufficiently large.

## Parameters
- `str`: Output buffer where the formatted string will be stored (must be large enough for result)
- `fmt`: Format string containing text and format specifiers
- `args`: Variable arguments list containing values to be formatted according to fmt

## Dependencies
- Functions called/Symbols referenced:
  - PrintfTarget (struct for managing output formatting)
  - [dopr](../d/dopr.md) (internal function that performs the actual formatting work)
- Called from (representative examples):
  - [pg_sprintf](pg_sprintf.md) (wrapper function for sprintf functionality)
  - vsprintf (when PostgreSQL's implementation is used instead of system's)
  - printf (indirectly through PostgreSQL printf wrappers)

## Notes and Other Information
- Returns the number of characters written (not counting the null terminator) if successful, or -1 on failure
- WARNING: This function performs no buffer bounds checking - the caller must ensure the buffer is large enough
- The PrintfTarget.bufend is set to NULL to indicate unlimited buffer mode to dopr()
- Always null-terminates the output string
- Should be avoided in favor of pg_vsnprintf for safer code - only use when buffer size is definitively known to be sufficient
- Part of PostgreSQL's portable printf implementation providing consistent behavior across platforms
- The nchars field is initialized but not really used since there's no buffer limit to track overflow

## Simplified Source

```c
int pg_vsprintf(char *str, const char *fmt, va_list args)
{
    PrintfTarget target;

    // Set up target for unlimited buffer formatting
    target.bufstart = target.bufptr = str;
    target.bufend = NULL;  // NULL indicates no size limit
    target.failed = false;

    // Perform the actual formatting
    dopr(&target, fmt, args);

    // Null-terminate the result
    *target.bufptr = '\0';

    // Return character count or -1 on failure
    return target.failed ? -1 : (target.bufptr - target.bufstart);
}
```