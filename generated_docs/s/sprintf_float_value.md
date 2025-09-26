# sprintf_float_value

## Location
[src/interfaces/ecpg/ecpglib/execute.c:472-487](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/execute.c#L472-L487)

## Overview
A static utility function that formats a single-precision floating-point value into a string representation, handling special IEEE 754 values like NaN and infinity with PostgreSQL-compatible formatting.

## Definition

```c
static void
sprintf_float_value(char *ptr, float value, const char *delim)
```
## Detailed Description
The  function provides specialized string formatting for single-precision floating-point numbers in ECPG applications. It mirrors the functionality of  but operates on float values instead of double values. The function ensures that special IEEE 754 floating-point values are represented in a PostgreSQL-compatible format:

- NaN (Not a Number) values are formatted as "NaN"
- Positive infinity is formatted as "Infinity"  
- Negative infinity is formatted as "-Infinity"
- Normal finite values use the "%.15g" format specifier for consistent precision representation

This function maintains uniformity in floating-point string representation across different precision levels in ECPG applications.

## Parameters / Member Variables
- : Pointer to the destination buffer where the formatted string will be written
- : The single-precision floating-point value to be formatted
- : Delimiter string to be appended after the formatted value

## Dependencies
- Functions called/Symbols referenced:
  - : Standard library function to test for NaN values
  - : Standard library function to test for infinite values
  - : Standard library function for formatted string output

- Called from (representative examples):
  - : Used for formatting float values in input processing (lines 727, 732)

## Notes and Other Information
- Identical logic to  but operates on single-precision float values
- Uses the same "%.15g" format specifier as the double version for consistent output formatting
- Handles both positive and negative infinity cases explicitly
- The delimiter parameter enables flexible formatting for various output contexts
- This function is static and only used within the execute.c module for internal formatting operations
- Despite using "%.15g", actual precision is limited by the float type's ~7 significant digits