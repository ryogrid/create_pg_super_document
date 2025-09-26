# dtoi2

## Location
[src/backend/utils/adt/float.c:1232-1256](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L1232-L1256)

## Overview
The dtoi2 function converts a float8 (double precision floating-point) number to an int2 (smallint) value, performing range checking and error handling for out-of-range values.

## Definition
```c
Datum dtoi2(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL built-in conversion function that safely converts floating-point numbers to 16-bit signed integers (smallint). The function implements PostgreSQL's standard approach to numeric type conversion:

1. Extracts the float8 input parameter using PostgreSQL's function call interface
2. Applies rounding using rint() to eliminate fractional parts, ensuring that values just outside the valid range but within rounding tolerance are handled correctly
3. Performs comprehensive range checking to ensure the value fits within the int16 range (-32768 to 32767)
4. Handles special floating-point values (NaN, Infinity) by raising appropriate errors
5. Returns the converted value using PostgreSQL's return macro

The function follows PostgreSQL's error handling conventions, throwing a NUMERIC_VALUE_OUT_OF_RANGE error when conversion is not possible.

## Parameters / Member Variables
- Input parameter (accessed via `PG_GETARG_FLOAT8(0)`): The float8 value to be converted to smallint

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8 (PostgreSQL macro to extract float8 argument)
  - rint (standard math function for rounding to nearest integer)
  - isnan (standard math function to check for NaN)
  - FLOAT8_FITS_IN_INT16 (PostgreSQL macro for range validation)
  - ereport (PostgreSQL error reporting function)
  - PG_RETURN_INT16 (PostgreSQL macro to return int16 value)
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- The function uses rint() instead of simple truncation to handle edge cases where floating-point values are just outside the valid integer range due to precision issues
- Special handling for NaN and Infinity values prevents undefined behavior during conversion
- Part of PostgreSQL's comprehensive type conversion system, located in src/backend/utils/adt/float.c
- The function signature follows PostgreSQL's version-1 calling convention for built-in functions
- [Range](../R/Range.md) validation uses the FLOAT8_FITS_IN_INT16 macro which likely checks against INT16_MIN and INT16_MAX boundaries