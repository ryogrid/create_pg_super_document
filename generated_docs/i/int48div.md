# int48div

## Location
src/backend/utils/adt/int8.c: 1013 - 1031

## Overview
Divides a 32-bit integer (int4) by a 64-bit integer (int8) and returns a 64-bit integer result with division by zero checking.

## Definition
```c
Datum int48div(PG_FUNCTION_ARGS)
```

## Detailed Description
The `int48div` function performs division of a 32-bit integer by a 64-bit integer. It takes two arguments through PostgreSQLs function calling convention: the first argument is a 32-bit integer (int4) dividend and the second is a 64-bit integer (int8) divisor. The function checks for division by zero and raises an appropriate error if detected. Since the dividend is smaller than the divisor type, no overflow is possible during the division operation.

## Parameters / Member Variables
- `arg1`: 32-bit integer (int4) dividend retrieved from function arguments
- `arg2`: 64-bit integer (int8) divisor retrieved from function arguments

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32: Retrieves the first 32-bit integer argument
  - PG_GETARG_INT64: Retrieves the second 64-bit integer argument
  - ereport: Reports error when division by zero occurs
  - PG_RETURN_NULL: Returns NULL (used for compiler safety, never actually reached)
  - PG_RETURN_INT64: Returns the 64-bit division result
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is part of PostgreSQLs arithmetic operations for mixed integer types
- Division by zero is explicitly checked and generates a DIVISION_BY_ZERO error
- No overflow checking is needed since dividing a smaller type by a larger type cannot overflow
- The PG_RETURN_NULL() call is included to help the compiler understand control flow but is never executed
- Located in src/backend/utils/adt/int8.c at lines 1013-1031