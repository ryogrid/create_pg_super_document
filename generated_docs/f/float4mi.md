# float4mi

## Location
[src/backend/utils/adt/float.c:730-738](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L730-L738)

## Overview
PostgreSQL SQL-callable function that performs subtraction of two single-precision floating-point numbers (float4), implementing the '-' operator for the float4 data type.

## Definition

```c
Datum
float4mi(PG_FUNCTION_ARGS)
```
## Detailed Description
The `float4mi` function is a PostgreSQL fmgr-compatible function that subtracts the second float4 value from the first float4 value. It extracts two float4 arguments from the function call context using PostgreSQL's function manager macros, delegates the actual arithmetic operation to the inline helper function `float4_mi`, and returns the result as a PostgreSQL Datum. This function provides overflow detection - if the subtraction of two finite values results in infinity, it raises a floating-point overflow error.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function manager argument structure containing:
  - arg1 (extracted as float4): Minuend (value to subtract from)
  - arg2 (extracted as float4): Subtrahend (value to be subtracted)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT4 (macro to extract float4 arguments)
  - [float4_mi](float4_mi.md) (inline helper function that performs the actual subtraction with overflow checking)
  - PG_RETURN_FLOAT4 (macro to return float4 result as Datum)
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's function manager system for the '-' operator)

## Notes and Other Information
- This is part of PostgreSQL's arithmetic operator implementation for the float4 data type
- The function uses PostgreSQL's fmgr (function manager) calling convention
- Overflow detection is performed by the underlying float4_mi function
- Located in src/backend/utils/adt/float.c alongside other float4 arithmetic operators
- The function signature follows PostgreSQL's standard pattern for SQL-callable functions

## Simplified Source

```c
Datum
float4mi(PG_FUNCTION_ARGS)
{
    // Extract two float4 arguments from function call
    float4 arg1 = PG_GETARG_FLOAT4(0);  // minuend
    float4 arg2 = PG_GETARG_FLOAT4(1);  // subtrahend

    // Perform subtraction with overflow checking
    float4 result = arg1 - arg2;
    if (isinf(result) && !isinf(arg1) && !isinf(arg2)) {
        // Overflow error if result is infinite but inputs were finite
        float_overflow_error();
    }

    PG_RETURN_FLOAT4(result);
}
```