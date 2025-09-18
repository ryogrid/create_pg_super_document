# pg_snprintf

## Location
src/port/snprintf.c: 202 - 213

## Overview
pg_snprintf is PostgreSQL's portable implementation of the snprintf function that formats and stores a string in a buffer using variable arguments.

## Definition


## Detailed Description
pg_snprintf provides a safe, portable alternative to the standard snprintf function. It is a variadic wrapper around pg_vsnprintf that accepts a variable number of arguments instead of a va_list. The function formats the format string `fmt` with the provided arguments and stores the result in the buffer `str`, ensuring proper null-termination and buffer bounds checking. This function serves as the main entry point for PostgreSQL's printf-style string formatting when you have direct arguments rather than a va_list.

## Parameters
- `str`: Output buffer where the formatted string will be stored
- `count`: Size of the output buffer (maximum number of characters to write, including null terminator)
- `fmt`: Format string containing text and format specifiers  
- `...`: Variable arguments containing values to be formatted according to fmt

## Dependencies
- Functions called/Symbols referenced:
  - pg_vsnprintf (performs the actual formatting work with va_list)
- Called from (representative examples):
  - initPopulateTable (in pgbench for table initialization)
  - snprintf (when PostgreSQL's implementation replaces system's)
  - printf (indirectly through PostgreSQL printf wrappers)

## Notes and Other Information
- Returns the number of characters that would have been written (not counting the null terminator) if successful, or -1 on failure
- This is a thin wrapper that converts variadic arguments to a va_list and delegates to pg_vsnprintf
- Provides the same safety guarantees as pg_vsnprintf including buffer bounds checking and null-termination
- Part of PostgreSQL's comprehensive portable printf implementation
- Used throughout PostgreSQL codebase where formatted string output to a buffer is needed with direct arguments