# float_to_shortest_decimal

## Location
[src/common/f2s.c:797-803](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/f2s.c#L797-L803)

## Overview
A memory-allocating convenience function that converts a single-precision floating-point number to its shortest decimal string representation and returns it as a dynamically allocated, null-terminated string.

## Definition
```c
char *float_to_shortest_decimal(float f)
```

## Detailed Description
This function provides the most convenient interface for floating-point to decimal conversion by handling memory allocation automatically. It allocates sufficient memory using `palloc()` (or `malloc()` outside the backend), performs the conversion using `float_to_shortest_decimal_buf()`, and returns a pointer to the null-terminated result string.

The function abstracts away buffer management concerns and provides a simple interface where the caller only needs to provide the float value and handle freeing the returned memory. This is particularly useful in contexts where temporary string representations are needed without pre-allocated buffers.

## Parameters / Member Variables
- `f`: The single-precision floating-point number to convert

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation function)
  - [float_to_shortest_decimal_buf](float_to_shortest_decimal_buf.md) (conversion function with caller-provided buffer)
  - FLOAT_SHORTEST_DECIMAL_LEN (buffer size constant)
- Called from:
  - FLOAT_SHORTEST_DECIMAL_LEN (macro context)

## Notes and Other Information
- Returns a dynamically allocated string that the caller must free
- Uses `palloc()` in PostgreSQL backend context, `malloc()` in standalone context
- Most convenient but least efficient option due to memory allocation overhead
- Suitable for cases where string lifetime needs to extend beyond the calling function
- The returned string is always null-terminated and ready for use with standard C string functions
- Memory allocation size is exactly FLOAT_SHORTEST_DECIMAL_LEN bytes, no waste
- Caller responsibility to free the returned pointer to avoid memory leaks