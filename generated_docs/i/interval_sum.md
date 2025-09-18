# interval_sum

## Location
src/backend/utils/adt/timestamp.c: 4207 - 4246

## Overview
Final function for the interval sum() aggregate that returns the total sum of interval values from accumulated state.

## Definition
```c
Datum interval_sum(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the final function for PostgreSQL's interval sum() aggregate. It takes the accumulated IntervalAggState and returns the final sum of all interval values. The function handles several cases:

1. Returns NULL if no non-null inputs were processed
2. Handles infinite intervals by checking for mixed positive and negative infinities (which produces an error)
3. Returns positive infinity if only positive infinite intervals were encountered
4. Returns negative infinity if only negative infinite intervals were encountered
5. For finite intervals only, returns a copy of the accumulated sum

The function ensures mathematical correctness when dealing with infinite intervals and provides appropriate error handling for undefined operations.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: PostgreSQL function argument macro containing:
  - Arg 0: IntervalAggState pointer (accumulated aggregate state)

## Dependencies
- Functions called/Symbols referenced:
  - IntervalAggState (struct type)
  - Interval (struct type)
  - IA_TOTAL_COUNT (macro for total count calculation)
  - PG_ARGISNULL (PostgreSQL macro)
  - PG_GETARG_POINTER (PostgreSQL macro)
  - PG_RETURN_NULL (PostgreSQL macro)
  - PG_RETURN_INTERVAL_P (PostgreSQL macro)
  - INTERVAL_NOEND (macro for positive infinity)
  - INTERVAL_NOBEGIN (macro for negative infinity)
  - ereport (error reporting function)
  - errcode (error code function)
  - errmsg (error message function)
  - palloc (memory allocation function)
  - memcpy (memory copy function)
- Called from (representative examples):
  - No direct references found (likely used through PostgreSQL's aggregate function infrastructure)

## Notes and Other Information
- This is a final function in PostgreSQL's aggregate framework, called after all values have been accumulated
- Handles infinite intervals with proper mathematical semantics
- Mixed positive and negative infinite intervals result in an "interval out of range" error since the result would be undefined
- For finite results, creates a new Interval struct and copies the accumulated sum
- Part of PostgreSQL's interval data type aggregate operations
- Simpler than interval_avg since no division is required - just returns the accumulated sum