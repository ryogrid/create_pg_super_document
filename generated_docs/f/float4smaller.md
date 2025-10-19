# float4smaller

## Location
[src/backend/utils/adt/float.c:627-649](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L627-L649)

## Overview
Returns the smaller of two single-precision floating-point numbers (float4) in PostgreSQL.

## Definition
```c
Datum float4smaller(PG_FUNCTION_ARGS)
```

## Detailed Description
The `float4smaller` function is a PostgreSQL built-in function that compares two float4 (single-precision floating-point) arguments and returns the smaller one. It uses the `float4_lt` function to perform the comparison and returns the appropriate value. This function is part of the float4 base operations in PostgreSQL's arithmetic system and is typically used to implement the SQL LEAST function for float4 values.

## Parameters / Member Variables
- `arg1`: first float4 input value
- `arg2`: second float4 input value
- `result`: local float4 variable to store the smaller value

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_FLOAT4` - macro to extract float4 arguments from function call
  - `PG_RETURN_FLOAT4` - macro to return float4 result as Datum
  - [float4_lt](float4_lt.md) - function to compare two float4 values for less-than relationship
  - `float4` - PostgreSQL type for single-precision floating-point numbers
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Located in src/backend/utils/adt/float.c:627-649
- Part of PostgreSQL's float4 base operations
- Uses the dedicated `float4_lt` comparison function rather than direct C comparison
- Follows PostgreSQL's function calling convention using PG_FUNCTION_ARGS macro
- Returns result using PostgreSQL's Datum system for type-safe value passing
- The function handles proper float4 comparison semantics including NaN handling through the `float4_lt` function
- Complementary to `float4larger` function for min/max operations

## Simplified Source

```c
Datum
float4smaller(PG_FUNCTION_ARGS)
{
    // Extract both float4 arguments
    float4 arg1 = PG_GETARG_FLOAT4(0);
    float4 arg2 = PG_GETARG_FLOAT4(1);

    // Return the smaller value using PostgreSQL's float4 comparison
    if (float4_lt(arg1, arg2))
        PG_RETURN_FLOAT4(arg1);
    else
        PG_RETURN_FLOAT4(arg2);
}
```