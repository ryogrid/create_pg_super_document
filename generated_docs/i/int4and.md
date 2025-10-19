# int4and

## Location
[src/backend/utils/adt/int.c:1393-1401](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L1393-L1401)

## Overview
Performs bitwise AND operation on two 32-bit signed integers (int32).

## Definition
```c
Datum int4and(PG_FUNCTION_ARGS)
```

## Detailed Description
The `int4and` function is a PostgreSQL built-in function that performs a bitwise AND operation on two 32-bit signed integers. It implements the bitwise AND operator (&) for the `int4` (integer) data type. The function uses PostgreSQL's function calling convention with `PG_FUNCTION_ARGS` and returns a `Datum` value containing the result.

This function is part of PostgreSQL's bit-pushing operators family, which includes AND, OR, XOR, NOT, and bit shift operations for integer types. The bitwise AND operation compares each bit position of the two operands and returns 1 only when both corresponding bits are 1, otherwise returns 0.

## Parameters / Member Variables
- Function uses `PG_FUNCTION_ARGS` convention:
  - `arg1`: First 32-bit signed integer (extracted from argument 0)
  - `arg2`: Second 32-bit signed integer (extracted from argument 1)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_INT32` - Macro to extract int32 arguments from function call
  - `PG_RETURN_INT32` - Macro to return int32 value as Datum
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Located in `src/backend/utils/adt/int.c:1393-1401`
- This function corresponds to the SQL `&` operator when used with integer values
- Part of PostgreSQL's bitwise operators for the int4/integer data type
- Uses standard PostgreSQL V1 function calling convention
- The operation is performed using C's native bitwise AND operator for efficiency
- Related functions include `int4or`, `int4xor`, `int4not`, `int4shl`, and `int4shr` for other bitwise operations
- Also has equivalent functions for int2 (smallint) data type: `int2and`, `int2or`, etc.

## Simplified Source

```c
Datum int4and(PG_FUNCTION_ARGS) {
    // Extract two 32-bit integers from function arguments
    int32 arg1 = PG_GETARG_INT32(0);
    int32 arg2 = PG_GETARG_INT32(1);

    // Perform bitwise AND operation and return result
    PG_RETURN_INT32(arg1 & arg2);
}
```