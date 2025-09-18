# float48gt

## Location
src/backend/utils/adt/float.c: 3900 - 3908

## Overview
PostgreSQL function that performs greater-than comparison between a float4 (single precision) and a float8 (double precision) value.

## Definition


## Detailed Description
The `float48gt` function implements the greater-than comparison operator for mixed-precision floating point types in PostgreSQL. It takes a float4 (4-byte single precision float) as the first argument and a float8 (8-byte double precision float) as the second argument, then determines if the first value is greater than the second value.

The function works by:
1. Extracting the float4 value from the first function argument
2. Extracting the float8 value from the second function argument  
3. Converting the float4 to float8 precision via casting
4. Delegating the actual comparison to the `float8_gt` function
5. Returning the boolean result

This function is part of PostgreSQL's type system that allows seamless comparison operations between different numeric precision types.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `arg1`: float4 value (single precision floating point number)
  - `arg2`: float8 value (double precision floating point number)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_FLOAT4`: Macro to extract float4 from function arguments
  - `PG_GETARG_FLOAT8`: Macro to extract float8 from function arguments
  - `float8_gt`: Core function that performs greater-than comparison on two float8 values
  - `PG_RETURN_BOOL`: Macro to return boolean result
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- Located in `src/backend/utils/adt/float.c:3900-3908`
- Part of PostgreSQL's arithmetic data type (ADT) system
- Handles mixed-precision comparisons by promoting the lower precision operand
- The actual comparison logic is delegated to `float8_gt` after type promotion
- Returns a Datum-wrapped boolean value as per PostgreSQL's function call convention