# int4pl

## Location
[src/backend/utils/adt/int.c:791-804](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L791-L804)

## Overview
A PostgreSQL function that implements safe addition of two int4 (integer) values with overflow detection and error handling.

## Definition
```c
Datum int4pl(PG_FUNCTION_ARGS)
```

## Detailed Description
This function performs addition of two 32-bit signed integers (int4) with built-in overflow protection. It uses PostgreSQL's safe arithmetic function `pg_add_s32_overflow` to detect potential integer overflow before it occurs. If the addition would result in an overflow (i.e., the result cannot be represented within the range of a 32-bit signed integer), the function raises a "numeric value out of range" error rather than returning an incorrect wrapped-around result.

The function follows PostgreSQL's standard approach of prioritizing data integrity and explicit error handling over performance, ensuring that arithmetic operations never silently produce incorrect results due to overflow conditions.

## Parameters / Member Variables
- `PG_GETARG_INT32(0)`: The first int4 operand (arg1)
- `PG_GETARG_INT32(1)`: The second int4 operand (arg2)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_add_s32_overflow](../p/pg_add_s32_overflow.md) (safe addition with overflow detection)
  - ereport (error reporting)
  - ERROR (error level)
  - [errcode](../e/errcode.md) (error code specification)
  - [errmsg](../e/errmsg.md) (error message)
  - ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- Located in src/backend/utils/adt/int.c:791-804
- Part of PostgreSQL's arithmetic operation functions for integer types
- The "pl" suffix stands for "plus", indicating this is the addition operator
- Uses PostgreSQL's overflow-safe arithmetic utilities to prevent silent data corruption
- Demonstrates PostgreSQL's philosophy of explicit error handling for edge cases
- The overflow check ensures compliance with SQL standards for numeric precision and error handling

## Simplified Source

```c
// Simplified version of int4pl
Datum int4pl(PG_FUNCTION_ARGS) {
    int32 first_operand = PG_GETARG_INT32(0);
    int32 second_operand = PG_GETARG_INT32(1);
    int32 result;

    // Perform safe addition with overflow detection
    if (unlikely(pg_add_s32_overflow(first_operand, second_operand, &result))) {
        ereport(ERROR, /* integer out of range error */);
    }

    PG_RETURN_INT32(result);
}
```

Key simplifications made:
- Used more descriptive variable names
- Simplified error handling for clarity
- Focused on the core safe arithmetic operation
- This function exemplifies PostgreSQL's approach to overflow-safe arithmetic