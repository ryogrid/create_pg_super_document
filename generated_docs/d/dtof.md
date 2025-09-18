# dtof

## Location
src/backend/utils/adt/float.c: 1188 - 1206

## Overview
A conversion function that converts a float8 (double precision) number to a float4 (single precision) number with overflow and underflow detection.

## Definition


## Detailed Description
This function performs type conversion from PostgreSQL's float8 data type (double precision floating point) to float4 data type (single precision floating point). Unlike the reverse conversion (ftod), this function requires careful handling of potential precision loss, overflow, and underflow conditions that can occur when narrowing from double to single precision. The function includes explicit checks for these edge cases and raises appropriate errors when the conversion would result in invalid values.

## Parameters / Member Variables
-  (float8): The double-precision floating-point number to be converted to single precision
-  (float4): The resulting single-precision value after conversion

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8 (double-precision argument extraction macro)
  - isinf (infinity detection function)
  - [float_overflow_error](../f/float_overflow_error.md) (overflow error reporting function)
  - [float_underflow_error](../f/float_underflow_error.md) (underflow error reporting function) 
  - PG_RETURN_FLOAT4 (single-precision return macro)
- Called from (representative examples):
  - No direct callers found (likely called through PostgreSQL's type conversion system)

## Notes and Other Information
- Requires overflow/underflow checking due to precision narrowing from double to single precision
- Detects overflow when the result becomes infinite but the input was finite
- Detects underflow when the result becomes zero but the input was non-zero
- Part of PostgreSQL's floating-point conversion routines section
- Uses unlikely() macro hints for performance optimization on error paths
- More complex than ftod due to potential data loss during precision reduction
- Source location: src/backend/utils/adt/float.c:1188-1206