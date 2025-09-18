# percentile_disc_final

## Location
src/backend/utils/adt/orderedsetaggs.c: 427 - 502

## Overview
Final function for the  ordered-set aggregate that calculates discrete percentiles by returning the first value whose position is at or above the specified percentile.

## Definition


## Detailed Description
The  function implements the final phase of the  aggregate. It calculates discrete percentiles, meaning it returns an actual value from the input set rather than interpolating between values (as  would).

The function first validates that the percentile argument is between 0 and 1. If there are no input rows or all input values were null, it returns null. Otherwise, it ensures the collected data is sorted and uses the mathematical formula  to determine which row to return, where N is the total number of non-null rows and K is the 1-based position of the desired row.

The function handles edge cases properly: for percentile 0, it returns the first value; for percentile 1, it returns the last value. The implementation skips K-1 rows and returns the Kth row, which corresponds to the smallest value whose cumulative distribution is greater than or equal to the requested percentile.

## Parameters / Member Variables
- : Standard PostgreSQL function arguments macro containing:
  - Argument 0: OSAPerGroupState pointer from transition function (may be null if no input rows)
  - Argument 1: Percentile value (float8, must be between 0.0 and 1.0)

## Dependencies
- Functions called/Symbols referenced:
  - AggCheckCallContext
  - tuplesort_performsort
  - tuplesort_rescan
  - tuplesort_skiptuples
  - tuplesort_getdatum
  - PG_ARGISNULL
  - PG_GETARG_POINTER
  - PG_GETARG_FLOAT8
  - PG_RETURN_NULL
  - PG_RETURN_DATUM
  - isnan
  - ceil
  - ereport
- Called from (representative examples):
  - PostgreSQL aggregate execution framework as final function for percentile_disc aggregate

## Notes and Other Information
- Returns discrete percentiles (actual input values) rather than interpolated values
- Validates percentile parameter is in range [0.0, 1.0] and not NaN
- Handles empty result sets and all-null inputs by returning null
- Uses  formula to determine the position of the result row
- Supports rescanning if the aggregate state is shared across execution nodes
- Part of SQL standard ordered-set aggregate functions
- Commonly used for median calculations: 