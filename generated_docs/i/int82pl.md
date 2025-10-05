# int82pl

## Location
[src/backend/utils/adt/int8.c:1032-1045](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L1032-L1045)

## Overview
Adds a 64-bit integer (int8) and a 16-bit integer (int2) and returns a 64-bit integer result with overflow checking.

## Definition
```c
Datum int82pl(PG_FUNCTION_ARGS)
```

## Detailed Description
The `int82pl` function performs addition between a 64-bit integer and a 16-bit integer. It takes two arguments through PostgreSQLs function calling convention: the first argument is a 64-bit integer (int8) and the second is a 16-bit integer (int2). The function converts the 16-bit integer to 64-bit and performs the addition with overflow detection. If overflow occurs, it raises an error with the message "bigint out of range".

## Parameters / Member Variables
- `arg1`: 64-bit integer (int8) operand retrieved from function arguments
- `arg2`: 16-bit integer (int2) operand retrieved from function arguments  
- `result`: 64-bit integer to store the addition result

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64: Retrieves the first 64-bit integer argument
  - PG_GETARG_INT16: Retrieves the second 16-bit integer argument
  - [pg_add_s64_overflow](../p/pg_add_s64_overflow.md): Performs 64-bit addition with overflow detection
  - ereport: Reports error when overflow occurs
  - PG_RETURN_INT64: Returns the 64-bit result
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is part of PostgreSQLs arithmetic operations for mixed integer types
- Uses PostgreSQLs overflow-safe addition function to prevent silent overflow
- Follows PostgreSQLs standard function calling convention using PG_FUNCTION_ARGS
- The smaller 16-bit operand is cast to 64-bit before the operation
- Located in src/backend/utils/adt/int8.c at lines 1032-1045

## Simplified Source

```c
Datum int82pl(PG_FUNCTION_ARGS) {
    int64 arg1 = PG_GETARG_INT64(0);  // Get first 64-bit argument
    int16 arg2 = PG_GETARG_INT16(1);  // Get second 16-bit argument
    int64 result;

    // Perform addition with overflow check (cast arg2 to 64-bit)
    if (pg_add_s64_overflow(arg1, (int64) arg2, &result))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                       errmsg("bigint out of range")));

    PG_RETURN_INT64(result);
}
```