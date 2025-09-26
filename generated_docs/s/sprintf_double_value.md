# sprintf_double_value

## Location
src/interfaces/ecpg/ecpglib/execute.c: 456 - 471

## Overview
A static utility function that formats a double-precision floating-point value into a string representation, handling special IEEE 754 values like NaN and infinity with PostgreSQL-compatible formatting.

## Definition

```c
static void
sprintf_double_value(char *ptr, double value, const char *delim)
```
## Detailed Description
The  function provides specialized string formatting for double-precision floating-point numbers in ECPG applications. It ensures that special IEEE 754 floating-point values are represented in a PostgreSQL-compatible format:

- NaN (Not a Number) values are formatted as "NaN"
- Positive infinity is formatted as "Infinity"
- Negative infinity is formatted as "-Infinity"
- Normal finite values use the "%.15g" format specifier for optimal precision

This function is essential for maintaining data consistency when converting floating-point values to their string representations for database operations or output formatting.

## Parameters / Member Variables
- : Pointer to the destination buffer where the formatted string will be written
- : The double-precision floating-point value to be formatted
- : Delimiter string to be appended after the formatted value

## Dependencies
- Functions called/Symbols referenced:
  - : Standard library function to test for NaN values
  - : Standard library function to test for infinite values
  - : Standard library function for formatted string output

- Called from (representative examples):
  - : Used for formatting double values in input processing (lines 746, 751)

## Notes and Other Information
- Uses IEEE 754 standard functions for reliable detection of special floating-point values
- The "%.15g" format provides up to 15 significant digits, which is appropriate for double precision
- Handles both positive and negative infinity cases explicitly
- The delimiter parameter allows flexible formatting for various output contexts
- This function is static and only used within the execute.c module for internal formatting operations