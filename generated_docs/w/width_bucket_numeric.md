# width_bucket_numeric

## Location
[src/backend/utils/adt/numeric.c:1845-1932](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L1845-L1932)

## Overview
Implements the numeric version of the SQL2003 width_bucket() function, which assigns a numeric operand to a bucket number in an equiwidth histogram with specified bounds and bucket count.

## Definition

```c
Datum
width_bucket_numeric(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function implements the width_bucket() function defined by SQL2003 for numeric data types. It takes an operand value and determines which bucket it belongs to in a histogram with specified lower bound, upper bound, and bucket count. The function creates an equiwidth histogram where:

- Values less than the lower bound are assigned to bucket 0
- Values greater than or equal to the upper bound are assigned to bucket (count+1) 
- Values within the bounds are assigned to buckets 1 through count based on their position

The function validates inputs to ensure count > 0, bounds are not equal, no parameters are NaN, and bounds are not infinite. It handles both ascending (bound1 < bound2) and descending (bound1 > bound2) histograms correctly.

## Parameters / Member Variables
- : The numeric value to assign to a bucket (parameter 0)
- : The first histogram bound (parameter 1)
- : The second histogram bound (parameter 2) 
- : The number of buckets in the histogram (parameter 3)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NUMERIC - Extract numeric parameters
  - PG_GETARG_INT32 - Extract integer parameter (count)
  - NUMERIC_IS_SPECIAL, NUMERIC_IS_NAN, NUMERIC_IS_INF - Check for special values
  - init_var - [Initialize](../I/Initialize.md) NumericVar variables
  - [int64_to_numericvar](../i/int64_to_numericvar.md) - Convert integer to NumericVar
  - [cmp_numerics](../c/cmp_numerics.md) - Compare numeric values
  - [set_var_from_var](../s/set_var_from_var.md) - Copy NumericVar values
  - [add_var](../a/add_var.md) - Add NumericVar values
  - [compute_bucket](../c/compute_bucket.md) - Calculate bucket assignment for values within bounds
  - [numericvar_to_int32](../n/numericvar_to_int32.md) - Convert result to 32-bit integer
  - [free_var](../f/free_var.md) - Clean up NumericVar memory
  - PG_RETURN_INT32 - Return integer result
- Called from:
  - No direct references found (typically called via SQL function calls)

## Notes and Other Information
- Located in src/backend/utils/adt/numeric.c:1845-1932
- Follows SQL2003 standard for width_bucket function behavior
- Handles edge cases: values outside bounds, equal bounds, invalid counts
- Supports both ascending and descending histogram ranges
- Validates all inputs for NaN and infinity constraints
- Returns bucket numbers as 32-bit integers
- Uses helper function  for values within the histogram bounds
- Part of PostgreSQL's statistical and analytical function suite