# int2abs

## Location
[src/backend/utils/adt/int.c:1205-1232](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L1205-L1232)

## Overview
Computes the absolute value of a 16-bit integer (smallint) with overflow protection.

## Definition
```c
Datum int2abs(PG_FUNCTION_ARGS)
```

## Detailed Description
The `int2abs` function implements PostgreSQL's absolute value operation for 16-bit integers (smallint). It safely computes the absolute value while protecting against integer overflow that would occur when attempting to negate the minimum value of a signed 16-bit integer (`PG_INT16_MIN`). When the input is the minimum possible 16-bit signed integer value, the function raises an error since the absolute value cannot be represented in the same data type.

## Parameters / Member Variables
- Input parameter (accessed via `PG_GETARG_INT16(0)`): The 16-bit integer whose absolute value is to be computed

## Dependencies
- Functions called/Symbols referenced:
  - `PG_INT16_MIN`: Constant representing the minimum value for a 16-bit signed integer
  - `PG_GETARG_INT16()`: PostgreSQL macro to extract int16 argument
  - `PG_RETURN_INT16()`: PostgreSQL macro to return int16 result
  - `ereport()`: PostgreSQL error reporting function
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- The function includes overflow protection by checking if the input equals `PG_INT16_MIN` (-32,768), which cannot be negated without overflow in 16-bit signed arithmetic
- Uses PostgreSQL's function calling convention with `PG_FUNCTION_ARGS` and return macros
- Part of PostgreSQL's integer arithmetic functions located in `src/backend/utils/adt/int.c`
- The `unlikely()` macro is used for branch prediction optimization on the overflow check
- Error message specifically mentions "smallint out of range" to indicate the 16-bit integer type