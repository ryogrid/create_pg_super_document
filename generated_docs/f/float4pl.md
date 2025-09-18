# float4pl

## Location
src/backend/utils/adt/float.c: 721 - 729

## Overview
PostgreSQL SQL-callable function that performs addition of two single-precision floating-point numbers (float4), implementing the '+' operator for the float4 data type.

## Definition


## Detailed Description
The  function is a PostgreSQL fmgr-compatible function that adds two float4 (single-precision floating-point) values. It extracts two float4 arguments from the function call context using PostgreSQL's function manager macros, delegates the actual arithmetic operation to the inline helper function , and returns the result as a PostgreSQL Datum. This function provides overflow detection - if the addition of two finite values results in infinity, it raises a floating-point overflow error.

## Parameters / Member Variables
- : Standard PostgreSQL function manager argument structure containing:
  - arg1 (extracted as float4): First operand for addition
  - arg2 (extracted as float4): Second operand for addition

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT4 (macro to extract float4 arguments)
  - float4_pl (inline helper function that performs the actual addition with overflow checking)
  - PG_RETURN_FLOAT4 (macro to return float4 result as Datum)
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's function manager system for the '+' operator)

## Notes and Other Information
- This is part of PostgreSQL's arithmetic operator implementation for the float4 data type
- The function uses PostgreSQL's fmgr (function manager) calling convention
- Overflow detection is performed by the underlying float4_pl function
- Located in src/backend/utils/adt/float.c alongside other float4 arithmetic operators
- The function signature follows PostgreSQL's standard pattern for SQL-callable functions