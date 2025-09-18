# int8abs

## Location
src/backend/utils/adt/int8.c: 546 - 562

## Overview
Computes the absolute value of a 64-bit signed integer (bigint) with overflow protection for the minimum value case.

## Definition


## Detailed Description
The int8abs function implements the absolute value operation for PostgreSQL's bigint data type. It takes a single int64 argument and returns its absolute value. The function includes critical overflow handling for the special case where the input is INT64_MIN, since the absolute value of this number cannot be represented in two's complement arithmetic (as |INT64_MIN| = INT64_MAX + 1). When this condition is detected, the function raises an error rather than returning an incorrect result.

## Parameters / Member Variables
- Uses  macro to access function arguments
- : Input value extracted as int64
- : Stores the absolute value result as int64

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64 (extracts int64 argument)
  - PG_RETURN_INT64 (returns int64 result)
  - PG_INT64_MIN (minimum int64 constant)
  - ereport (error reporting)
  - [errcode](../e/errcode.md)/errmsg (error code and message macros)
- Called from:
  - No direct references found (likely called via PostgreSQL function dispatch system)

## Notes and Other Information
- Uses unlikely() hint for the overflow case to optimize the common path
- INT64_MIN input raises ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE error
- Implements conditional negation: returns -arg1 if negative, arg1 if positive or zero
- Critical for maintaining mathematical correctness in two's complement systems
- Part of PostgreSQL's comprehensive bigint arithmetic operations