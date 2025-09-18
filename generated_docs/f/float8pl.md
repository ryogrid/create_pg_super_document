# float8pl

## Location
src/backend/utils/adt/float.c: 763 - 771

## Overview
PostgreSQL SQL-callable function that performs addition of two double-precision floating-point numbers (float8), implementing the '+' operator for the float8 data type.

## Definition
```c
Datum float8pl(PG_FUNCTION_ARGS)
```

## Detailed Description
The `float8pl` function is a PostgreSQL fmgr-compatible function that adds two float8 (double-precision floating-point) values. It extracts two float8 arguments from the function call context using PostgreSQL's function manager macros, delegates the actual arithmetic operation to the inline helper function `float8_pl`, and returns the result as a PostgreSQL Datum. This function provides overflow detection - if the addition of two finite values results in infinity, it raises a floating-point overflow error.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function manager argument structure containing:
  - arg1 (extracted as float8): First operand for addition
  - arg2 (extracted as float8): Second operand for addition

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8 (macro to extract float8 arguments)
  - [float8_pl](float8_pl.md) (inline helper function that performs the actual addition with overflow checking)
  - PG_RETURN_FLOAT8 (macro to return float8 result as Datum)
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's function manager system for the '+' operator)

## Notes and Other Information
- This is part of PostgreSQL's arithmetic operator implementation for the float8 data type
- The function uses PostgreSQL's fmgr (function manager) calling convention
- Overflow detection is performed by the underlying float8_pl function
- Located in src/backend/utils/adt/float.c alongside other float8 arithmetic operators
- The function signature follows PostgreSQL's standard pattern for SQL-callable functions
- Similar to float4pl but operates on double-precision (64-bit) floating-point values instead of single-precision (32-bit)