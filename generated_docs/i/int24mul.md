# int24mul

## Location
[src/backend/utils/adt/int.c:1016-1029](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L1016-L1029)

## Overview
 is a PostgreSQL function that performs multiplication between a 16-bit integer (int2/smallint) and a 32-bit integer (int4/integer), returning a 32-bit integer result with overflow checking.

## Definition

```c
Datum
int24mul(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the multiplication operator for mixed-precision integer arithmetic in PostgreSQL's type system. It takes a 16-bit integer as the first argument and a 32-bit integer as the second argument, performs safe multiplication with overflow detection, and returns the result as a 32-bit integer. The function uses PostgreSQL's safe arithmetic functions to prevent integer overflow, throwing an error if the result would exceed the range of a 32-bit signed integer.

## Parameters / Member Variables
- : 16-bit signed integer (int2/smallint) - the first multiplicand
- : 32-bit signed integer (int4/integer) - the second multiplicand
- : 32-bit signed integer to store the multiplication result

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT16 - extracts 16-bit integer argument
  - PG_GETARG_INT32 - extracts 32-bit integer argument  
  - [pg_mul_s32_overflow](../p/pg_mul_s32_overflow.md) - performs safe 32-bit integer multiplication with overflow detection
  - PG_RETURN_INT32 - returns 32-bit integer result
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Located in src/backend/utils/adt/int.c:1016-1029
- Part of PostgreSQL's arithmetic operator system for mixed integer types
- Throws ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE error if multiplication would overflow
- The function follows PostgreSQL's function call convention using PG_FUNCTION_ARGS
- Promotes the 16-bit argument to 32-bit before performing the multiplication operation
- Part of the int24 family of functions (int24pl, int24mi, int24mul, int24div) for mixed-precision arithmetic

## Simplified Source

```c
Datum int24mul(PG_FUNCTION_ARGS) {
    int16 arg1 = PG_GETARG_INT16(0);  // int2 multiplicand
    int32 arg2 = PG_GETARG_INT32(1);  // int4 multiplicand
    int32 result;

    // Multiply with int2 promoted to int4, check for overflow
    if (unlikely(pg_mul_s32_overflow((int32) arg1, arg2, &result)))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                       errmsg("integer out of range")));

    PG_RETURN_INT32(result);
}
```