# ftoi4

## Location
src/backend/utils/adt/float.c: 1281 - 1305

## Overview
The ftoi4 function converts a float4 (single precision floating-point) number to an int4 (32-bit signed integer), performing range checking and error handling for values outside the representable integer range.

## Definition
```c
Datum ftoi4(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides safe conversion from single precision floating-point numbers to 32-bit signed integers within PostgreSQL's type system. The conversion process involves several critical steps to ensure data integrity:

1. Extracts the float4 input parameter using PostgreSQL's function argument interface
2. Applies rounding using rint() to eliminate fractional parts, ensuring that floating-point values just outside the integer range due to precision issues are handled gracefully
3. Performs comprehensive range validation to verify the value fits within the int32 range (-2,147,483,648 to 2,147,483,647)
4. Handles special IEEE 754 values (NaN, positive/negative infinity) by detecting them and raising appropriate errors
5. Returns the converted integer value using PostgreSQL's standard return mechanism

The function follows PostgreSQL's error reporting conventions, generating a NUMERIC_VALUE_OUT_OF_RANGE error when the conversion cannot be performed safely.

## Parameters / Member Variables
- Input parameter (accessed via `PG_GETARG_FLOAT4(0)`): The float4 value to be converted to int4

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT4 (PostgreSQL macro to extract float4 argument)
  - rint (standard math function for rounding to nearest integer)
  - isnan (standard math function to detect NaN values)
  - FLOAT4_FITS_IN_INT32 (PostgreSQL macro for range validation)
  - ereport (PostgreSQL error reporting function)
  - PG_RETURN_INT32 (PostgreSQL macro to return int32 value)
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- Uses rint() rather than truncation to handle edge cases where floating-point representation places values just outside the valid integer range
- The FLOAT4_FITS_IN_INT32 macro likely checks against INT32_MIN and INT32_MAX boundaries while handling floating-point precision issues
- Special floating-point values (NaN, ±Infinity) are explicitly detected to prevent undefined behavior during conversion
- Part of PostgreSQL's comprehensive type conversion system located in src/backend/utils/adt/float.c
- Follows PostgreSQL's version-1 calling convention for built-in functions
- The conversion may result in precision loss when the float4 value has fractional components, which are rounded away
- Commonly used in SQL operations that require integer results from floating-point calculations