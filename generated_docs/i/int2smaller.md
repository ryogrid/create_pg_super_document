# int2smaller

## Location
[src/backend/utils/adt/int.c:1355-1363](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L1355-L1363)

## Overview
Returns the smaller of two 16-bit signed integers (int16).

## Definition
```c
Datum int2smaller(PG_FUNCTION_ARGS)
```

## Detailed Description
The `int2smaller` function is a PostgreSQL built-in function that compares two 16-bit signed integers and returns the smaller value. It implements the MIN operation for the `int2` (smallint) data type. The function uses PostgreSQL's function calling convention with `PG_FUNCTION_ARGS` and returns a `Datum` value containing the result.

The implementation is straightforward: it extracts two int16 arguments from the function call arguments, compares them using a simple conditional expression, and returns the smaller value wrapped in a PostgreSQL Datum.

## Parameters / Member Variables
- Function uses `PG_FUNCTION_ARGS` convention:
  - `arg1`: First 16-bit signed integer (extracted from argument 0)
  - `arg2`: Second 16-bit signed integer (extracted from argument 1)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_INT16` - Macro to extract int16 arguments from function call
  - `PG_RETURN_INT16` - Macro to return int16 value as Datum
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Located in `src/backend/utils/adt/int.c:1355-1363`
- This function corresponds to the SQL `LEAST()` function when used with two smallint values
- Part of PostgreSQL's arithmetic and comparison operators for the int2/smallint data type
- Uses standard PostgreSQL V1 function calling convention
- The comparison is performed using simple C conditional operator for efficiency