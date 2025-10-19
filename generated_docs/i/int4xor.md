# int4xor

## Location
[src/backend/utils/adt/int.c:1411-1419](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L1411-L1419)

## Overview
Performs bitwise XOR (exclusive OR) operation on two 32-bit integer values and returns the result as a PostgreSQL function.

## Definition
```c
Datum int4xor(PG_FUNCTION_ARGS)
```

## Detailed Description
The int4xor function implements the bitwise XOR (exclusive OR) operator (^) for PostgreSQL's integer type (int4). It takes two 32-bit signed integer arguments and returns their bitwise XOR result. This function is part of PostgreSQL's built-in integer bitwise operations and is typically invoked through SQL's bitwise XOR operator for integers.

The function uses PostgreSQL's function call convention with PG_FUNCTION_ARGS macro to access arguments and PG_RETURN_INT32 macro to return the result in the proper Datum format.

## Parameters / Member Variables
- `arg1` (PG_GETARG_INT32(0)): First 32-bit signed integer operand
- `arg2` (PG_GETARG_INT32(1)): Second 32-bit signed integer operand

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32 (macro for extracting int32 arguments)
  - PG_RETURN_INT32 (macro for returning int32 result)
- Called from (representative examples):
  - SQL queries using bitwise XOR operations on integer values
  - Internal PostgreSQL operator evaluation system

## Notes and Other Information
- This function is located in src/backend/utils/adt/int.c:1411-1419
- Part of PostgreSQL's arithmetic and bitwise operations for the int4 data type
- The bitwise XOR operation sets each bit to 1 if exactly one of the corresponding bits in the operands is 1
- [Result](../R/Result.md) follows standard C bitwise XOR semantics for 32-bit signed integers

## Simplified Source

```c
Datum int4xor(PG_FUNCTION_ARGS) {
    // Extract two 32-bit integers from function arguments
    int32 arg1 = PG_GETARG_INT32(0);
    int32 arg2 = PG_GETARG_INT32(1);

    // Perform bitwise XOR operation and return result
    PG_RETURN_INT32(arg1 ^ arg2);
}
```