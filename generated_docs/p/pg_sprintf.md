# pg_sprintf

## Location
[src/port/snprintf.c:230-241](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/snprintf.c#L230-L241)

## Overview
pg_sprintf is PostgreSQL's portable implementation of sprintf that formats a string into an unbounded buffer using variable arguments.

## Definition


## Detailed Description
pg_sprintf provides a portable alternative to the standard sprintf function. It is a variadic wrapper around pg_vsprintf that accepts a variable number of arguments instead of a va_list. The function formats the format string `fmt` with the provided arguments and stores the result in the buffer `str`. Like pg_vsprintf, this function assumes the output buffer is large enough to hold the entire formatted string and performs no bounds checking. This function should only be used when you can guarantee the buffer is sufficiently large.

## Parameters
- `str`: Output buffer where the formatted string will be stored (must be large enough for result)
- `fmt`: Format string containing text and format specifiers
- `...`: Variable arguments containing values to be formatted according to fmt

## Dependencies
- Functions called/Symbols referenced:
  - [pg_vsprintf](pg_vsprintf.md) (performs the actual formatting work with va_list)
- Called from (representative examples):
  - sprintf (when PostgreSQL's implementation replaces system's)
  - printf (indirectly through PostgreSQL printf wrappers)

## Notes and Other Information
- Returns the number of characters written (not counting the null terminator) if successful, or -1 on failure
- WARNING: This function performs no buffer bounds checking - the caller must ensure the buffer is large enough
- This is a thin wrapper that converts variadic arguments to a va_list and delegates to pg_vsprintf
- Should be avoided in favor of pg_snprintf for safer code - only use when buffer size is definitively known to be sufficient
- Part of PostgreSQL's comprehensive portable printf implementation
- Always null-terminates the output string
- Used when formatted string output to an unbounded buffer is needed with direct arguments