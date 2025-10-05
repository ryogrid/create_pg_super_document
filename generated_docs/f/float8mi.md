# float8mi

## Location
[src/backend/utils/adt/float.c:772-780](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L772-L780)

## Overview
PostgreSQL function that performs subtraction of two double-precision floating-point numbers (float8) and returns the result as a Datum for use in SQL operations.

## Definition

```c
Datum
float8mi(PG_FUNCTION_ARGS)
```
## Detailed Description
float8mi is a PostgreSQL built-in function wrapper that implements the subtraction operator (-) for double-precision floating-point numbers in SQL. It extracts two float8 arguments from the function call arguments, performs subtraction using the inline helper function float8_mi(), and returns the result wrapped in a Datum. The function includes overflow detection to handle cases where finite operands produce infinite results, which would indicate arithmetic overflow.

## Parameters / Member Variables
- : Standard PostgreSQL function calling convention macro that provides access to function arguments and context
  - arg1 (float8): First operand (minuend) - the number from which another is subtracted
  - arg2 (float8): Second operand (subtrahend) - the number being subtracted

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8: Macro to extract float8 arguments from function call
  - [float8_mi](float8_mi.md): Inline helper function that performs the actual subtraction with overflow checking
  - PG_RETURN_FLOAT8: Macro to return float8 result as Datum
- Called from (representative examples):
  - No direct references found (likely called through SQL operator dispatch)

## Notes and Other Information
- This function serves as the SQL-callable wrapper for the minus operator between float8 values
- The actual arithmetic is delegated to float8_mi() which includes overflow detection
- Part of PostgreSQL's type system for double-precision floating-point arithmetic
- Located in src/backend/utils/adt/float.c:772-780

## Simplified Source

```c
Datum
float8mi(PG_FUNCTION_ARGS)
{
    // Get the two float8 operands
    float8 arg1 = PG_GETARG_FLOAT8(0);  // minuend
    float8 arg2 = PG_GETARG_FLOAT8(1);  // subtrahend

    // Perform subtraction and return result
    PG_RETURN_FLOAT8(float8_mi(arg1, arg2));
}
```