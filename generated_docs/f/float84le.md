# float84le

## Location
src/backend/utils/adt/float.c: 3948 - 3956

## Overview
Function that compares a float8 (double precision) value with a float4 (single precision) value to determine if the first is less than or equal to the second.

## Definition
Datum float84le(PG_FUNCTION_ARGS)

## Detailed Description
The float84le function implements the less-than-or-equal-to comparison operator between a float8 (double precision floating point) value and a float4 (single precision floating point) value. It extracts the float8 argument from the first position and the float4 argument from the second position, then delegates the actual comparison to the float8_le function after casting the float4 argument to float8. This function is typically used by PostgreSQL's operator system to handle mixed-precision floating point comparisons.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to function arguments and context
  - Argument 0: float8 value (double precision)
  - Argument 1: float4 value (single precision)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8: Extracts float8 argument from function arguments
  - PG_GETARG_FLOAT4: Extracts float4 argument from function arguments
  - float8_le: Performs the actual less-than-or-equal comparison between two float8 values
  - PG_RETURN_BOOL: Returns boolean result to PostgreSQL
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function handles mixed-precision comparisons by promoting the float4 argument to float8 before comparison
- Part of PostgreSQL's floating point arithmetic operator family
- Returns a boolean Datum indicating whether arg1 <= arg2
- Located in src/backend/utils/adt/float.c:3948-3956