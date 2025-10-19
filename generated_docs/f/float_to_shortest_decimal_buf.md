# float_to_shortest_decimal_buf

## Location
[src/common/f2s.c:780-796](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/f2s.c#L780-L796)

## Overview
A wrapper function that converts a single-precision floating-point number to its shortest decimal string representation and stores it as a null-terminated string in the caller-supplied buffer.

## Definition
```c
int float_to_shortest_decimal_buf(float f, char *result)
```

## Detailed Description
This function serves as a convenient wrapper around `float_to_shortest_decimal_bufn()` that adds null termination to the result string. It performs the same floating-point to decimal conversion but ensures the output is a proper C-style null-terminated string, making it suitable for standard string operations.

The function delegates the actual conversion work to `float_to_shortest_decimal_bufn()` and then adds a null terminator at the end of the generated string. It includes an assertion to verify that the buffer is large enough to accommodate both the decimal representation and the null terminator.

## Parameters / Member Variables
- `f`: The single-precision floating-point number to convert
- `result`: Output buffer that must be at least FLOAT_SHORTEST_DECIMAL_LEN bytes long (includes space for null terminator)

## Dependencies
- Functions called/Symbols referenced:
  - [float_to_shortest_decimal_bufn](float_to_shortest_decimal_bufn.md) (core conversion function)
  - FLOAT_SHORTEST_DECIMAL_LEN (buffer size constant)
  - Assert (debugging macro)
- Called from:
  - [float4out](float4out.md) (PostgreSQL float4 output function)
  - [float_to_shortest_decimal](float_to_shortest_decimal.md) (convenience wrapper)
  - FLOAT_SHORTEST_DECIMAL_LEN (macro context)

## Notes and Other Information
- This is the null-terminated version of the conversion function, unlike `float_to_shortest_decimal_bufn()` 
- Buffer requirement is FLOAT_SHORTEST_DECIMAL_LEN bytes (one more than the non-terminated version)
- Returns the string length (excluding the null terminator)
- Contains assertion to ensure buffer bounds are respected in debug builds
- Commonly used in PostgreSQL for float4 data type output formatting
- The null termination makes the result suitable for use with standard C string functions

## Simplified Source

```c
int float_to_shortest_decimal_buf(float f, char *result) {
    // Convert float to decimal string (without null terminator)
    const int length = float_to_shortest_decimal_bufn(f, result);

    // Add null terminator to make it a proper C string
    result[length] = '\0';

    return length;  // Length excluding null terminator
}
```