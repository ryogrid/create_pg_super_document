# int2mul

## Location
src/backend/utils/adt/int.c: 934 - 948

## Overview
A PostgreSQL system function that implements multiplication for 16-bit signed integers (int2/smallint) with overflow detection and error handling.

## Definition
```c
Datum int2mul(PG_FUNCTION_ARGS)
```

## Detailed Description
The int2mul function implements the binary multiplication operator (*) for PostgreSQL's int2 (smallint) data type. It takes two int16 values as input and returns their product, with built-in overflow detection to prevent arithmetic overflow errors. If the multiplication would result in a value outside the valid range for int16 (-32768 to 32767), the function raises a PostgreSQL error with the message "smallint out of range".

Multiplication is particularly prone to overflow since the product of two numbers can be much larger than either operand. The function uses PostgreSQL's safe arithmetic functions (pg_mul_s16_overflow) to detect overflow conditions before they occur, ensuring data integrity and preventing undefined behavior.

## Parameters / Member Variables
- `arg1`: First int16 operand (multiplicand) obtained via PG_GETARG_INT16(0)
- `arg2`: Second int16 operand (multiplier) obtained via PG_GETARG_INT16(1)
- `result`: Local variable to store the multiplication result

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT16 (macro for extracting int16 arguments)
  - [pg_mul_s16_overflow](../p/pg_mul_s16_overflow.md) (safe multiplication with overflow detection)
  - ereport (error reporting function)
  - [errcode](../e/errcode.md)/errmsg (error handling macros)
  - PG_RETURN_INT16 (macro for returning int16 value)
- Called from (representative examples):
  - No direct references found in the codebase (likely called through operator dispatch)

## Notes and Other Information
- Located in src/backend/utils/adt/int.c:934-948
- This function is typically invoked through PostgreSQL's operator system when the * operator is used with two smallint values
- Throws ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE error if overflow occurs
- Part of PostgreSQL's arithmetic operator family for the int2/smallint data type
- Uses the unlikely() macro hint to optimize for the common case where overflow does not occur
- Multiplication overflow can happen more easily than addition/subtraction since the result grows quadratically with the operand values