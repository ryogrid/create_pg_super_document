# numeric_trim_scale

## Location
src/backend/utils/adt/numeric.c: 4223 - 4243

## Overview
Reduces the scale of a numeric value to its minimum required representation without loss of precision by removing trailing zeros.

## Definition


## Detailed Description
The `numeric_trim_scale` function is a PostgreSQL built-in function that creates a new numeric value with trailing zeros removed from the fractional part. It calculates the minimum scale needed to represent the value accurately and returns a new numeric with that reduced scale. For special values (NaN, infinity), it returns a duplicate of the original value. This function is useful for normalizing numeric values and reducing storage overhead.

## Parameters / Member Variables
- The function uses the standard PostgreSQL function calling convention `PG_FUNCTION_ARGS`
- Input: A single numeric value accessed via `PG_GETARG_NUMERIC(0)`

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_NUMERIC` - Extracts numeric argument from function call
  - `NUMERIC_IS_SPECIAL` - Checks if numeric value is special (NaN, infinity)
  - `duplicate_numeric` - Creates a copy of special numeric values
  - `init_var_from_num` - Converts external Numeric to internal NumericVar
  - `get_min_scale` - Calculates the minimum required scale
  - `make_result` - Converts NumericVar back to external Numeric format
  - `free_var` - Releases memory allocated for NumericVar
  - `PG_RETURN_NUMERIC` - Returns numeric result
- Called from (representative examples):
  - Currently no direct references found in the analyzed codebase

## Notes and Other Information
- For special values, returns a duplicate rather than attempting scale reduction
- The function creates a new numeric value rather than modifying the input
- Useful for normalizing numeric representations (e.g., 1.2000 becomes 1.2)
- Can help reduce storage requirements for numeric values with unnecessary trailing zeros
- Properly manages memory by freeing temporary NumericVar structures
- Part of PostgreSQL's numeric data type utility functions
- Located in src/backend/utils/adt/numeric.c:4223-4243