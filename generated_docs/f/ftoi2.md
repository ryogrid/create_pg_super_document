# ftoi2

## Location
[src/backend/utils/adt/float.c:1306-1330](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L1306-L1330)

## Overview
The ftoi2 function converts a float4 (single precision floating-point) number to an int2 (smallint, 16-bit signed integer), performing range validation and error handling for values outside the representable smallint range.

## Definition
```c
Datum ftoi2(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides safe conversion from single precision floating-point numbers to 16-bit signed integers within PostgreSQL's type system. The conversion process implements several safeguards to ensure data integrity and prevent overflow:

1. Extracts the float4 input parameter using PostgreSQL's function argument interface
2. Applies rounding using rint() to eliminate fractional parts, ensuring that floating-point values just slightly outside the smallint range due to representation precision are handled correctly
3. Performs strict range validation to verify the value fits within the int16 range (-32,768 to 32,767)
4. Detects and handles special IEEE 754 values (NaN, positive/negative infinity) by raising appropriate errors
5. Returns the converted smallint value using PostgreSQL's standard return mechanism

The function follows PostgreSQL's error handling standards, generating a NUMERIC_VALUE_OUT_OF_RANGE error with a "smallint out of range" message when conversion cannot be performed safely.

## Parameters / Member Variables
- Input parameter (accessed via `PG_GETARG_FLOAT4(0)`): The float4 value to be converted to int2 (smallint)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT4 (PostgreSQL macro to extract float4 argument)
  - rint (standard math function for rounding to nearest integer)
  - isnan (standard math function to detect NaN values)
  - FLOAT4_FITS_IN_INT16 (PostgreSQL macro for range validation against smallint bounds)
  - ereport (PostgreSQL error reporting function)
  - PG_RETURN_INT16 (PostgreSQL macro to return int16 value)
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- Uses rint() instead of truncation to handle boundary cases where floating-point representation places values just outside the valid smallint range
- The FLOAT4_FITS_IN_INT16 macro likely performs bounds checking against INT16_MIN (-32,768) and INT16_MAX (32,767)
- Special floating-point values are explicitly detected to prevent undefined behavior during integer conversion
- Part of PostgreSQL's comprehensive type conversion system in src/backend/utils/adt/float.c
- Follows PostgreSQL's version-1 calling convention for built-in functions
- This is a narrowing conversion that may lose precision when the float4 has fractional components
- The smallint range is much smaller than int32, making overflow more likely and requiring careful validation
- Commonly used when precise integer storage in minimal space is required from floating-point calculations