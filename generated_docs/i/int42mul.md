# int42mul

## Location
[src/backend/utils/adt/int.c:1077-1090](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L1077-L1090)

## Overview
Multiplies a 32-bit integer by a 16-bit integer, returning a 32-bit result with overflow checking.

## Definition
```c
Datum int42mul(PG_FUNCTION_ARGS)
```

## Detailed Description
The `int42mul` function implements multiplication between a 32-bit integer (int4) and a 16-bit integer (int2), returning the result as a 32-bit integer. The function performs overflow checking using PostgreSQL's safe arithmetic operations to ensure that the multiplication does not exceed the range of a 32-bit signed integer. If overflow is detected, it raises an error with code ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE.

## Parameters / Member Variables
- `arg1`: The 32-bit integer multiplicand
- `arg2`: The 16-bit integer multiplier, cast to 32-bit for the operation
- `result`: The 32-bit integer result of the multiplication

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_INT32`: Extracts the first 32-bit integer argument
  - `PG_GETARG_INT16`: Extracts the second 16-bit integer argument
  - [pg_mul_s32_overflow](../p/pg_mul_s32_overflow.md): Performs safe 32-bit integer multiplication with overflow checking
  - `ereport`: Reports errors when overflow occurs
  - `PG_RETURN_INT32`: Returns the 32-bit result
- Called from: No direct references found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL's arithmetic operator implementation for mixed integer types
- The function follows PostgreSQL's function call convention using PG_FUNCTION_ARGS
- Overflow checking ensures mathematical safety and prevents undefined behavior
- The int16 argument is implicitly cast to int32 before the multiplication operation

## Simplified Source

```c
Datum int42mul(PG_FUNCTION_ARGS) {
    int32 arg1 = PG_GETARG_INT32(0);  // Get 32-bit multiplicand
    int16 arg2 = PG_GETARG_INT16(1);  // Get 16-bit multiplier
    int32 result;

    // Perform multiplication with overflow check
    if (pg_mul_s32_overflow(arg1, (int32) arg2, &result))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                       errmsg("integer out of range")));

    PG_RETURN_INT32(result);
}
```