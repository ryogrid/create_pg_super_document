# numeric_poly_stddev_pop

## Location
src/backend/utils/adt/numeric.c: 6483 - 6523

## Overview
Computes the population standard deviation of numeric values using an optimized polynomial aggregation state when 128-bit integer support is available.

## Definition
```c
Datum numeric_poly_stddev_pop(PG_FUNCTION_ARGS)
```

## Detailed Description
This function calculates the population standard deviation for numeric data types. Like its variance counterpart, it uses PostgreSQL's optimized polynomial aggregation state (PolyNumAggState) when 128-bit integer support is available, providing better performance for large datasets. When 128-bit integers are not available, it falls back to the standard numeric_stddev_pop implementation.

The function extracts the aggregation state and delegates computation to numeric_poly_stddev_internal with parameters indicating this is a standard deviation (not variance) calculation for the entire population (not a sample).

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing the aggregation state as argument 0

## Dependencies
- Functions called/Symbols referenced:
  - [numeric_poly_stddev_internal](numeric_poly_stddev_internal.md) (when HAVE_INT128 is defined)
  - [numeric_stddev_pop](numeric_stddev_pop.md) (fallback when HAVE_INT128 is not defined)
  - PG_ARGISNULL
  - PG_GETARG_POINTER
  - PG_RETURN_NULL
  - PG_RETURN_NUMERIC
- Called from:
  - No direct references found (likely called through PostgreSQL's function manager)

## Notes and Other Information
- Part of PostgreSQL's optimized numeric aggregation system for handling large datasets efficiently
- Uses conditional compilation to select the best available implementation based on platform capabilities
- Population standard deviation calculation uses the entire dataset without Bessel's correction
- The function is nearly identical to numeric_poly_var_pop except it passes false for the variance parameter to compute standard deviation instead
- Located in src/backend/utils/adt/numeric.c:6483-6523