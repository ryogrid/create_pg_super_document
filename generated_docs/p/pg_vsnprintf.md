# pg_vsnprintf

## Location
[src/port/snprintf.c:174-201](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/snprintf.c#L174-L201)

## Overview
pg_vsnprintf is PostgreSQL's portable implementation of the vsnprintf function that formats and stores a string in a buffer with a va_list argument.

## Definition

```c
int
pg_vsnprintf(char *str, size_t count, const char *fmt, va_list args)
```
## Detailed Description
pg_vsnprintf provides a safe, portable alternative to the standard vsnprintf function. It formats the format string `fmt` with the variable arguments contained in `args` and stores the result in the buffer `str`. The function ensures null-termination and handles edge cases like zero-length buffers by substituting a temporary one-byte buffer when count is 0 (following C99 standard). The actual formatting work is delegated to the internal `dopr` function which provides comprehensive printf-style formatting capabilities including support for positional parameters (%n$), various format specifiers, and proper error handling.

## Parameters
- `str`: Output buffer where the formatted string will be stored
- `count`: Size of the output buffer (maximum number of characters to write, including null terminator) 
- `fmt`: Format string containing text and format specifiers
- `args`: Variable arguments list containing values to be formatted according to fmt

## Dependencies
- Functions called/Symbols referenced:
  - PrintfTarget (struct for managing output formatting)
  - [dopr](../d/dopr.md) (internal function that performs the actual formatting work)
- Called from (representative examples):
  - [pg_snprintf](pg_snprintf.md) (wrapper function for snprintf functionality)
  - vsnprintf (when PostgreSQL's implementation is used instead of system's)
  - printf (indirectly through other PostgreSQL printf wrappers)

## Notes and Other Information
- Returns the number of characters that would have been written (not counting the null terminator) if successful, or -1 on failure
- The function preserves errno value until reaching dopr() to ensure proper error reporting
- Handles the C99 edge case where str is NULL and count is 0 by using a local buffer
- Always null-terminates the output string within the specified buffer size
- Part of PostgreSQL's portable printf implementation that provides consistent behavior across platforms
- Uses a PrintfTarget structure to track buffer state, character counts, and error conditions