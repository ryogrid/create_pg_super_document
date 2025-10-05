# width_bucket_float8

## Location
[src/backend/utils/adt/float.c:3989-4082](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L3989-L4082)

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

## Simplified Source

```c
Datum width_bucket_float8(PG_FUNCTION_ARGS) {
    // Extract arguments
    float8 operand = PG_GETARG_FLOAT8(0);
    float8 bound1 = PG_GETARG_FLOAT8(1);
    float8 bound2 = PG_GETARG_FLOAT8(2);
    int32 count = PG_GETARG_INT32(3);
    int32 result;

    // Validate inputs: count > 0, no NaN values, finite bounds
    if (count <= 0 || isnan(operand) || isnan(bound1) || isnan(bound2) ||
        isinf(bound1) || isinf(bound2) || bound1 == bound2) {
        ereport(ERROR, /* appropriate error */);
    }

    if (bound1 < bound2) {
        // Ascending histogram
        if (operand < bound1)
            result = 0;  // Below range
        else if (operand >= bound2)
            result = count + 1;  // Above range
        else {
            // Calculate bucket within range
            if (!isinf(bound2 - bound1)) {
                result = count * ((operand - bound1) / (bound2 - bound1));
            } else {
                // Handle overflow case by dividing by 2
                result = count * ((operand/2 - bound1/2) / (bound2/2 - bound1/2));
            }
            // Ensure result is in valid range and add 1
            if (result >= count) result = count - 1;
            result++;
        }
    } else {
        // Descending histogram (bound1 > bound2)
        if (operand > bound1)
            result = 0;
        else if (operand <= bound2)
            result = count + 1;
        else {
            // Calculate bucket for descending range
            if (!isinf(bound1 - bound2))
                result = count * ((bound1 - operand) / (bound1 - bound2));
            else
                result = count * ((bound1/2 - operand/2) / (bound1/2 - bound2/2));
            if (result >= count) result = count - 1;
            result++;
        }
    }

    PG_RETURN_INT32(result);
}
```