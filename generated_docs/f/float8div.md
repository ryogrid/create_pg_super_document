# float8div

## Location
src/backend/utils/adt/float.c: 790 - 808

## Overview
PostgreSQL function that performs division of two double-precision floating-point numbers (float8) and returns the result as a Datum for use in SQL operations.

## Definition
```c
Datum float8div(PG_FUNCTION_ARGS)
```

## Detailed Description
float8div is a PostgreSQL built-in function wrapper that implements the division operator (/) for double-precision floating-point numbers in SQL. It extracts two float8 arguments from the function call arguments, performs division using the inline helper function float8_div(), and returns the result wrapped in a Datum. The function includes comprehensive error detection for division by zero, overflow (finite dividend produces infinite result), and underflow (non-zero dividend with finite divisor produces zero result).

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function calling convention macro that provides access to function arguments and context
  - arg1 (float8): First operand (dividend) - the number being divided
  - arg2 (float8): Second operand (divisor) - the number by which the dividend is divided

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8: Macro to extract float8 arguments from function call
  - float8_div: Inline helper function that performs the actual division with comprehensive error checking
  - PG_RETURN_FLOAT8: Macro to return float8 result as Datum
- Called from (representative examples):
  - No direct references found (likely called through SQL operator dispatch)

## Notes and Other Information
- This function serves as the SQL-callable wrapper for the division operator between float8 values
- The actual arithmetic is delegated to float8_div() which includes multiple error checks:
  - Division by zero detection (when divisor is 0.0 and dividend is not NaN)
  - Overflow detection (when finite dividend produces infinite result)
  - Underflow detection (when non-zero dividend with finite divisor produces zero result)
- Part of PostgreSQL's type system for double-precision floating-point arithmetic
- Located in src/backend/utils/adt/float.c:790-808