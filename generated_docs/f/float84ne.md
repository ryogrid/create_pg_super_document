# float84ne

## Location
[src/backend/utils/adt/float.c:3930-3938](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L3930-L3938)

## Overview
PostgreSQL function that performs not-equal comparison between a float8 (double precision) and a float4 (single precision) value.

## Definition


## Detailed Description
The `float84ne` function implements the not-equal comparison operator for mixed-precision floating point types in PostgreSQL. It takes a float8 (8-byte double precision float) as the first argument and a float4 (4-byte single precision float) as the second argument, then determines if the two values are not equal.

The function works by:
1. Extracting the float8 value from the first function argument
2. Extracting the float4 value from the second function argument  
3. Converting the float4 to float8 precision via casting
4. Delegating the actual comparison to the `float8_ne` function
5. Returning the boolean result

This function is part of PostgreSQL's type system that allows seamless comparison operations between different numeric precision types. Note that this is the reverse parameter order compared to the float48xx functions, handling float8/float4 comparisons instead of float4/float8.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `arg1`: float8 value (double precision floating point number)
  - `arg2`: float4 value (single precision floating point number)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_FLOAT8`: Macro to extract float8 from function arguments
  - `PG_GETARG_FLOAT4`: Macro to extract float4 from function arguments
  - [float8_ne](float8_ne.md): Core function that performs not-equal comparison on two float8 values
  - `PG_RETURN_BOOL`: Macro to return boolean result
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- Located in `src/backend/utils/adt/float.c:3930-3938`
- Part of PostgreSQL's arithmetic data type (ADT) system
- Handles mixed-precision comparisons by promoting the lower precision operand
- The actual comparison logic is delegated to `float8_ne` after type promotion
- Returns a Datum-wrapped boolean value as per PostgreSQL's function call convention
- Part of a family of float84xx comparison functions that handle float8/float4 operations