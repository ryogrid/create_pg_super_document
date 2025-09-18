# double_to_shortest_decimal_buf

## Location
src/common/d2s.c: 1053 - 1069

## Overview
Converts a double-precision floating-point number to its shortest decimal representation as a null-terminated string, providing a convenient wrapper around the core conversion function.

## Definition


## Detailed Description
This function provides a null-terminated string version of double-to-decimal conversion. It acts as a thin wrapper around `double_to_shortest_decimal_bufn`, adding only the null termination step. The function ensures that the resulting string is properly terminated for standard C string operations while maintaining the efficiency of the underlying conversion algorithm.

The function includes an assertion to verify that the conversion result fits within the expected buffer size, providing a safety check against buffer overruns.

## Parameters / Member Variables
- `f`: The double-precision floating-point number to convert
- `result`: Caller-provided buffer to store the null-terminated decimal string (must be at least DOUBLE_SHORTEST_DECIMAL_LEN bytes)

## Dependencies
- Functions called/Symbols referenced:
  - [double_to_shortest_decimal_bufn](double_to_shortest_decimal_bufn.md): Core conversion function for unterminated strings
  - `DOUBLE_SHORTEST_DECIMAL_LEN`: Maximum buffer size constant
  - `Assert`: Debug assertion macro for bounds checking
- Called from (representative examples):
  - [outDouble](../o/outDouble.md): Node output formatting function
  - [float8out_internal](../f/float8out_internal.md): Float8 datatype output function
  - [double_to_shortest_decimal](double_to_shortest_decimal.md): Memory-allocating wrapper function

## Notes and Other Information
- Returns the string length (excluding the null terminator)
- Buffer must be pre-allocated by caller with at least DOUBLE_SHORTEST_DECIMAL_LEN bytes
- Includes runtime assertion to verify the conversion result fits in expected bounds
- Preferred interface for callers who need null-terminated strings
- Minimal overhead compared to the core conversion function (just adds null termination)