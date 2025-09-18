# double_to_shortest_decimal

## Location
src/common/d2s.c: 1070 - 1076

## Overview
Converts a double-precision floating-point number to its shortest decimal representation as a dynamically allocated null-terminated string, providing the most convenient interface for callers.

## Definition


## Detailed Description
This function provides the highest-level interface for double-to-decimal conversion in PostgreSQL. It handles memory allocation automatically, using `palloc` in the backend (or `malloc` outside the backend) to create a properly sized buffer, then delegates the actual conversion to `double_to_shortest_decimal_buf`. 

This design separates memory management concerns from the conversion algorithm, allowing callers to focus on using the result without worrying about buffer allocation. The function is particularly useful for one-off conversions where the caller doesn't want to manage buffer allocation.

## Parameters / Member Variables
- `f`: The double-precision floating-point number to convert

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md): PostgreSQL memory allocation function (malloc outside backend)
  - [double_to_shortest_decimal_buf](double_to_shortest_decimal_buf.md): Null-terminated string conversion function
  - `DOUBLE_SHORTEST_DECIMAL_LEN`: Buffer size constant for allocation
- Called from:
  - Referenced by `DOUBLE_SHORTEST_DECIMAL_LEN` constant definition (indicating usage in size calculations)

## Notes and Other Information
- Returns a pointer to the allocated null-terminated string
- Caller is responsible for freeing the returned memory using `pfree` (or `free` outside backend)
- Most convenient interface but has memory allocation overhead
- Uses PostgreSQL's memory context system for proper memory management
- Automatically handles buffer sizing to prevent overflow
- Suitable for cases where memory allocation overhead is acceptable compared to convenience