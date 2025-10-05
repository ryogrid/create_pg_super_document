# float8out_internal

## Location
[src/backend/utils/adt/float.c:530-548](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L530-L548)

## Overview
Internal implementation function that converts double precision floating-point values to their string representation, providing platform-independent output formatting with configurable precision control.

## Definition

```c
char *
float8out_internal(double num)
```
## Detailed Description
This function serves as the core implementation for converting double-precision floating-point numbers to string format. It provides platform-independent output formatting with two modes of operation:

1. **High-precision mode** (when ): Uses  to generate the shortest decimal representation that will round-trip correctly
2. **Standard mode**: Uses  with a precision based on 

The function allocates memory for the result string and is designed to be reusable across different PostgreSQL data types that need to format floating-point values as part of their output.

## Parameters / Member Variables
- `num`: The double-precision floating-point value to convert to string format
## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation)
  - [double_to_shortest_decimal_buf](../d/double_to_shortest_decimal_buf.md) (shortest decimal representation)
  - [pg_strfromd](../p/pg_strfromd.md) (PostgreSQL's snprintf-style double formatting)
  
- Global variables used:
  - extra_float_digits (configuration parameter controlling output precision)
  - DBL_DIG (system constant for double precision digits)

- Called from (representative examples):
  - [float8out](float8out.md) (main float8 output function)
  - [single_encode](../s/single_encode.md) (geometric operations)
  - [pair_encode](../p/pair_encode.md) (geometric operations)  
  - [line_out](../l/line_out.md) (line type output function)

## Notes and Other Information
- Always returns a palloc'd string that must be freed by the caller
- [Result](../R/Result.md) string buffer is allocated with 32 bytes, sufficient for any double representation
- Precision is controlled by the  GUC parameter
- When , uses the shortest representation for better round-trip accuracy
- Designed for reuse in composite geometric types (point, line, etc.) that contain floating-point coordinates
- Platform-independent alternative to standard C library functions like sprintf/snprintf for double formatting

## Simplified Source

```c
char *
float8out_internal(double num)
{
    // Allocate buffer for result string
    char *result = (char *) palloc(32);

    // Choose output format based on extra_float_digits setting
    if (extra_float_digits > 0) {
        // Use shortest decimal representation for better precision
        double_to_shortest_decimal_buf(num, result);
    } else {
        // Use standard precision format
        int digits = DBL_DIG + extra_float_digits;
        pg_strfromd(result, 32, digits, num);
    }

    return result;
}
```