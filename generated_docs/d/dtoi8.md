# dtoi8

## Location
[src/backend/utils/adt/int8.c:1297-1317](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L1297-L1317)

## Overview
Converts a PostgreSQL float8 (double-precision floating-point) value to an int8 (64-bit integer) with range checking and error handling.

## Definition
```c
Datum dtoi8(PG_FUNCTION_ARGS)
```

## Detailed Description
This function performs a type conversion from PostgreSQL's double-precision floating-point type (float8) to 64-bit integer (int8). It first removes any fractional part using rint() to handle edge cases where values just outside the valid range might round into the acceptable range. The function then performs comprehensive range checking to ensure the resulting value fits within the int64 range, throwing an error if the input is NaN, infinite, or outside the valid integer range.

## Parameters / Member Variables
- The function uses PostgreSQL's `PG_FUNCTION_ARGS` macro to access arguments
- Argument 0: A float8 (double-precision floating-point) value to be converted

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8 (macro to extract float8 argument)
  - rint (C standard library function to round to nearest integer)
  - isnan (C standard library function to check for NaN)
  - FLOAT8_FITS_IN_INT64 (PostgreSQL macro for range checking)
  - ereport (PostgreSQL error reporting function)
  - PG_RETURN_INT64 (macro to return int64 result)
- Called from (representative examples):
  - [int8_to_char](../i/int8_to_char.md) (in src/backend/utils/adt/formatting.c:6676)

## Notes and Other Information
- Located in src/backend/utils/adt/int8.c:1297-1317
- This is a PostgreSQL built-in function that can be invoked from SQL
- Uses rint() to handle fractional values and edge cases gracefully
- Includes robust error handling for out-of-range values, NaN, and infinity
- Throws ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE error with "bigint out of range" message for invalid inputs
- Part of PostgreSQL's type system for safe numeric conversions