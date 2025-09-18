# width_bucket_float8

## Location
src/backend/utils/adt/float.c: 3989 - 4082

## Overview
Implements the float8 version of the SQL2003 width_bucket() function, which assigns a value to a histogram bucket number based on specified bounds and bucket count.

## Definition
Datum width_bucket_float8(PG_FUNCTION_ARGS)

## Detailed Description
The width_bucket_float8 function implements the SQL2003 standard width_bucket() function for float8 (double precision) values. It determines which bucket in an equiwidth histogram a given operand belongs to, based on lower and upper bounds and the total number of buckets. The function handles various edge cases including values outside the bounds, potential overflow conditions, and special handling for very large bound differences that could cause floating-point overflow. Values smaller than the lower bound are assigned to bucket 0, while values greater than or equal to the upper bound are assigned to bucket count+1.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro providing access to:
  - Argument 0: `operand` (float8) - The value to be assigned to a bucket
  - Argument 1: `bound1` (float8) - First bound of the histogram range
  - Argument 2: `bound2` (float8) - Second bound of the histogram range  
  - Argument 3: `count` (int32) - Number of buckets in the histogram

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8: Extracts float8 arguments from function parameters
  - PG_GETARG_INT32: Extracts int32 argument for bucket count
  - isnan: Checks for NaN values in floating-point arguments
  - isinf: Checks for infinite values in floating-point arguments
  - [pg_add_s32_overflow](../p/pg_add_s32_overflow.md): Safely adds integers with overflow checking
  - ereport: Reports errors with specific error codes
  - PG_RETURN_INT32: Returns integer result to PostgreSQL
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Implements SQL2003 standard width_bucket() function specification
- Validates input parameters: count must be > 0, no NaN values allowed, bounds must be finite
- Handles both ascending (bound1 < bound2) and descending (bound1 > bound2) histogram ranges
- Special overflow-safe computation for cases where bound difference exceeds DBL_MAX
- Returns bucket numbers from 0 to count+1, where 0 and count+1 are for out-of-bounds values
- Uses precise floating-point arithmetic with special handling for extreme values
- Located in src/backend/utils/adt/float.c:3989-4082
- Part of PostgreSQL's statistical and analytical function suite