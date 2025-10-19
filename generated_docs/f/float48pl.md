# float48pl

## Location
[src/backend/utils/adt/float.c:3777-3785](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L3777-L3785)

## Overview
Performs addition between a float4 (single-precision) and float8 (double-precision) value, returning a float8 result.

## Definition
```c
Datum float48pl(PG_FUNCTION_ARGS)
```

## Detailed Description
The `float48pl` function implements mixed-precision floating-point addition for PostgreSQL. It takes a float4 (4-byte single-precision) value as the first argument and a float8 (8-byte double-precision) value as the second argument. The function promotes the float4 value to float8 precision and then performs addition using the `float8_pl` function, which includes overflow checking.

This function is part of PostgreSQL's mixed-precision arithmetic operators that handle operations between different floating-point precisions. The result is always returned in the higher precision format (float8) to prevent loss of precision.

## Parameters / Member Variables
- `arg1`: float4 value (first operand) - single-precision floating-point number
- `arg2`: float8 value (second operand) - double-precision floating-point number

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT4
  - PG_GETARG_FLOAT8
  - [float8_pl](float8_pl.md) (performs the actual addition with overflow checking)
- Called from (representative examples):
  - Used in SQL expressions mixing float4 and float8 types
  - PostgreSQL operator system for "+" operator between float4 and float8

## Notes and Other Information
- Part of PostgreSQL's mixed-precision arithmetic operator family (float48pl, float48mi, float48mul, float48div)
- Promotes the float4 argument to float8 precision before performing the operation
- Inherits overflow detection from float8_pl, which throws an error if the result overflows to infinity
- The function signature follows PostgreSQL's function calling convention using PG_FUNCTION_ARGS
- [Result](../R/Result.md) precision is determined by the higher-precision operand (float8)
- Ensures mathematical operations between different float types are handled consistently

## Simplified Source

```c
Datum float48pl(PG_FUNCTION_ARGS) {
    // Get arguments: float4 + float8
    float4 arg1 = PG_GETARG_FLOAT4(0);  // Single-precision operand
    float8 arg2 = PG_GETARG_FLOAT8(1);  // Double-precision operand

    // Promote float4 to float8 and add using float8_pl
    PG_RETURN_FLOAT8(float8_pl((float8) arg1, arg2));
}
```