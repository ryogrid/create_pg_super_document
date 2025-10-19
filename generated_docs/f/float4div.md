# float4div

## Location
[src/backend/utils/adt/float.c:748-762](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L748-L762)

## Overview
PostgreSQL SQL-callable function that performs division of two single-precision floating-point numbers (float4), implementing the '/' operator for the float4 data type.

## Definition
```c
Datum float4div(PG_FUNCTION_ARGS)
```

## Detailed Description
The `float4div` function is a PostgreSQL fmgr-compatible function that divides the first float4 value by the second float4 value. It extracts two float4 arguments from the function call context using PostgreSQL's function manager macros, delegates the actual arithmetic operation to the inline helper function `float4_div`, and returns the result as a PostgreSQL Datum. This function provides comprehensive error checking including division by zero detection, overflow detection, and underflow detection.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function manager argument structure containing:
  - arg1 (extracted as float4): Dividend (value to be divided)
  - arg2 (extracted as float4): Divisor (value to divide by)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT4 (macro to extract float4 arguments)
  - [float4_div](float4_div.md) (inline helper function that performs the actual division with comprehensive error checking)
  - PG_RETURN_FLOAT4 (macro to return float4 result as Datum)
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's function manager system for the '/' operator)

## Notes and Other Information
- This is part of PostgreSQL's arithmetic operator implementation for the float4 data type
- The function uses PostgreSQL's fmgr (function manager) calling convention
- Division by zero, overflow, and underflow detection are performed by the underlying float4_div function
- Division by zero raises an error unless the dividend is NaN
- Located in src/backend/utils/adt/float.c alongside other float4 arithmetic operators
- The function signature follows PostgreSQL's standard pattern for SQL-callable functions

## Simplified Source

```c
Datum
float4div(PG_FUNCTION_ARGS)
{
    // Extract two float4 arguments from function call
    float4 arg1 = PG_GETARG_FLOAT4(0);  // dividend
    float4 arg2 = PG_GETARG_FLOAT4(1);  // divisor

    // Check for division by zero (unless dividend is NaN)
    if (arg2 == 0.0f && !isnan(arg1)) {
        float_zero_divide_error();
    }

    // Perform division
    float4 result = arg1 / arg2;

    // Check for overflow: result infinite but dividend was finite
    if (isinf(result) && !isinf(arg1)) {
        float_overflow_error();
    }

    // Check for underflow: result zero but dividend non-zero and divisor finite
    if (result == 0.0f && arg1 != 0.0f && !isinf(arg2)) {
        float_underflow_error();
    }

    PG_RETURN_FLOAT4(result);
}
```