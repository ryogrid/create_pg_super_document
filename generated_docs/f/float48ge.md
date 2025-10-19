# float48ge

## Location
[src/backend/utils/adt/float.c:3909-3920](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L3909-L3920)

## Overview
PostgreSQL function that performs greater-than-or-equal comparison between a float4 (single precision) and a float8 (double precision) value.

## Definition

```c
Datum
float48ge(PG_FUNCTION_ARGS)
```
## Detailed Description
The `float48ge` function implements the greater-than-or-equal comparison operator for mixed-precision floating point types in PostgreSQL. It takes a float4 (4-byte single precision float) as the first argument and a float8 (8-byte double precision float) as the second argument, then determines if the first value is greater than or equal to the second value.

The function works by:
1. Extracting the float4 value from the first function argument
2. Extracting the float8 value from the second function argument  
3. Converting the float4 to float8 precision via casting
4. Delegating the actual comparison to the `float8_ge` function
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
  - [float8_ge](float8_ge.md): Core function that performs greater-than-or-equal comparison on two float8 values
  - `PG_RETURN_BOOL`: Macro to return boolean result
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- Located in `src/backend/utils/adt/float.c:3909-3920`
- Part of PostgreSQL's arithmetic data type (ADT) system
- Handles mixed-precision comparisons by promoting the lower precision operand
- The actual comparison logic is delegated to `float8_ge` after type promotion
- Returns a Datum-wrapped boolean value as per PostgreSQL's function call convention

## Simplified Source

```c
Datum float48ge(PG_FUNCTION_ARGS) {
    // Extract float4 and float8 arguments
    float4 arg1 = PG_GETARG_FLOAT4(0);
    float8 arg2 = PG_GETARG_FLOAT8(1);

    // Convert float4 to float8 and compare for greater-than-or-equal
    PG_RETURN_BOOL(float8_ge((float8) arg1, arg2));
}
```