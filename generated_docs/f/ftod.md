# ftod

## Location
src/backend/utils/adt/float.c: 1176 - 1187

## Overview
A conversion function that converts a float4 (single precision) number to a float8 (double precision) number.

## Definition


## Detailed Description
This function performs a simple type conversion from PostgreSQL's float4 data type (single precision floating point) to float8 data type (double precision floating point). The conversion is straightforward, utilizing C's built-in type casting to promote the single-precision value to double precision. This function is part of PostgreSQL's type conversion infrastructure and enables implicit and explicit conversions between floating-point precisions.

## Parameters / Member Variables
-  (float4): The single-precision floating-point number to be converted to double precision

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT4 (single-precision argument extraction macro)
  - PG_RETURN_FLOAT8 (double-precision return macro)
- Called from (representative examples):
  - No direct callers found (likely called through PostgreSQL's type conversion system)

## Notes and Other Information
- Simple conversion function that relies on C's automatic precision promotion
- Part of PostgreSQL's floating-point conversion routines section
- Used internally by PostgreSQL's type system for implicit and explicit casts
- The conversion preserves all precision from the original float4 value
- No special handling needed for NaN or infinity values as C casting handles these correctly
- Source location: src/backend/utils/adt/float.c:1176-1187