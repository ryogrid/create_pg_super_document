# int4or

## Location
[src/backend/utils/adt/int.c:1402-1410](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L1402-L1410)

## Overview
Performs bitwise OR operation on two 32-bit integer values and returns the result as a PostgreSQL function.

## Definition

```c
Datum
int4or(PG_FUNCTION_ARGS)
```
## Detailed Description
The int4or function implements the bitwise OR operator (|) for PostgreSQL's integer type (int4). It takes two 32-bit signed integer arguments and returns their bitwise OR result. This function is part of PostgreSQL's built-in integer arithmetic operations and is typically invoked through SQL's # operator for integers.

The function uses PostgreSQL's function call convention with PG_FUNCTION_ARGS macro to access arguments and PG_RETURN_INT32 macro to return the result in the proper Datum format.

## Parameters / Member Variables
-  (PG_GETARG_INT32(0)): First 32-bit signed integer operand
-  (PG_GETARG_INT32(1)): Second 32-bit signed integer operand

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32 (macro for extracting int32 arguments)
  - PG_RETURN_INT32 (macro for returning int32 result)
- Called from (representative examples):
  - SQL queries using the # bitwise OR operator on integer values
  - Internal PostgreSQL operator evaluation system

## Notes and Other Information
- This function is located in src/backend/utils/adt/int.c:1402-1410
- Part of PostgreSQL's arithmetic and bitwise operations for the int4 data type
- The bitwise OR operation sets each bit to 1 if at least one of the corresponding bits in either operand is 1
- [Result](../R/Result.md) follows standard C bitwise OR semantics for 32-bit signed integers