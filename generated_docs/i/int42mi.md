# int42mi

## Location
[src/backend/utils/adt/int.c:1063-1076](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L1063-L1076)

## Overview
Subtracts a 16-bit integer from a 32-bit integer, returning a 32-bit result with overflow checking.

## Definition

```c
Datum
int42mi(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function implements subtraction between a 32-bit integer (int4) and a 16-bit integer (int2), returning the result as a 32-bit integer. The function performs overflow checking using PostgreSQL's safe arithmetic operations to ensure that the subtraction does not exceed the range of a 32-bit signed integer. If overflow is detected, it raises an error with code ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE.

## Parameters / Member Variables
- : The 32-bit integer minuend (value from which another is subtracted)
- : The 16-bit integer subtrahend (value to be subtracted), cast to 32-bit for the operation
- : The 32-bit integer result of the subtraction

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts the first 32-bit integer argument
  - : Extracts the second 16-bit integer argument
  - : Performs safe 32-bit integer subtraction with overflow checking
  - : Reports errors when overflow occurs
  - : Returns the 32-bit result
- Called from: No direct references found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL's arithmetic operator implementation for mixed integer types
- The function follows PostgreSQL's function call convention using PG_FUNCTION_ARGS
- Overflow checking ensures mathematical safety and prevents undefined behavior
- The int16 argument is implicitly cast to int32 before the subtraction operation

## Simplified Source

```c
Datum int42mi(PG_FUNCTION_ARGS) {
    int32 arg1 = PG_GETARG_INT32(0);  // Get 32-bit minuend
    int16 arg2 = PG_GETARG_INT16(1);  // Get 16-bit subtrahend
    int32 result;

    // Perform subtraction with overflow check
    if (pg_sub_s32_overflow(arg1, (int32) arg2, &result))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                       errmsg("integer out of range")));

    PG_RETURN_INT32(result);
}
```