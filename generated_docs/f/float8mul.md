# float8mul

## Location
src/backend/utils/adt/float.c: 781 - 789

## Overview
PostgreSQL function that performs multiplication of two double-precision floating-point numbers (float8) and returns the result as a Datum for use in SQL operations.

## Definition
```c
Datum float8mul(PG_FUNCTION_ARGS)
```

## Detailed Description
float8mul is a PostgreSQL built-in function wrapper that implements the multiplication operator (*) for double-precision floating-point numbers in SQL. It extracts two float8 arguments from the function call arguments, performs multiplication using the inline helper function float8_mul(), and returns the result wrapped in a Datum. The function includes both overflow and underflow detection to handle cases where finite operands produce infinite results (overflow) or where non-zero operands produce zero results (underflow).

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function calling convention macro that provides access to function arguments and context
  - arg1 (float8): First operand (multiplicand) - the first number to be multiplied
  - arg2 (float8): Second operand (multiplier) - the second number to be multiplied

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8: Macro to extract float8 arguments from function call
  - float8_mul: Inline helper function that performs the actual multiplication with overflow and underflow checking
  - PG_RETURN_FLOAT8: Macro to return float8 result as Datum
- Called from (representative examples):
  - No direct references found (likely called through SQL operator dispatch)

## Notes and Other Information
- This function serves as the SQL-callable wrapper for the multiplication operator between float8 values
- The actual arithmetic is delegated to float8_mul() which includes both overflow and underflow detection
- Overflow detection catches cases where finite inputs produce infinite results
- Underflow detection catches cases where non-zero inputs produce zero results
- Part of PostgreSQL's type system for double-precision floating-point arithmetic
- Located in src/backend/utils/adt/float.c:781-789