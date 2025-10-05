# int42pl

## Location
[src/backend/utils/adt/int.c:1049-1062](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L1049-L1062)

## Overview
 is a PostgreSQL function that performs addition between a 32-bit integer (int4/integer) and a 16-bit integer (int2/smallint), returning a 32-bit integer result with overflow checking.

## Definition

```c
Datum
int42pl(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the addition operator for mixed-precision integer arithmetic in PostgreSQL's type system, with the operand order reversed compared to int24pl. It takes a 32-bit integer as the first argument and a 16-bit integer as the second argument, performs safe addition with overflow detection, and returns the result as a 32-bit integer. The function uses PostgreSQL's safe arithmetic functions to prevent integer overflow, throwing an error if the result would exceed the range of a 32-bit signed integer.

## Parameters / Member Variables
- : 32-bit signed integer (int4/integer) - the first operand
- : 16-bit signed integer (int2/smallint) - the second operand  
- : 32-bit signed integer to store the addition result

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32 - extracts 32-bit integer argument
  - PG_GETARG_INT16 - extracts 16-bit integer argument
  - [pg_add_s32_overflow](../p/pg_add_s32_overflow.md) - performs safe 32-bit integer addition with overflow detection
  - PG_RETURN_INT32 - returns 32-bit integer result
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Located in src/backend/utils/adt/int.c:1049-1062
- Part of PostgreSQL's arithmetic operator system for mixed integer types
- Throws ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE error if addition would overflow
- The function follows PostgreSQL's function call convention using PG_FUNCTION_ARGS
- Promotes the 16-bit argument to 32-bit before performing the addition operation
- Complementary function to int24pl, handling the opposite operand order (int4 + int2 vs int2 + int4)
- Provides commutative addition support for mixed integer type operations in PostgreSQL

## Simplified Source

```c
Datum int42pl(PG_FUNCTION_ARGS) {
    int32 arg1 = PG_GETARG_INT32(0);  // Get 32-bit integer
    int16 arg2 = PG_GETARG_INT16(1);  // Get 16-bit integer
    int32 result;

    // Perform addition with overflow check
    if (pg_add_s32_overflow(arg1, (int32) arg2, &result))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                       errmsg("integer out of range")));

    PG_RETURN_INT32(result);
}
```