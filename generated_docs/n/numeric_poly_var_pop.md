# numeric_poly_var_pop

## Location
src/backend/utils/adt/numeric.c: 6462 - 6482

## Overview
Computes the population variance of numeric values using an optimized polynomial aggregation state when 128-bit integer support is available.

## Definition


## Detailed Description
This function calculates the population variance for numeric data types. It leverages PostgreSQL's optimized polynomial aggregation state (PolyNumAggState) when the system has 128-bit integer support (HAVE_INT128), which provides better performance for large datasets. When 128-bit integers are not available, it falls back to the standard numeric_var_pop implementation.

The function extracts the aggregation state from the first argument and delegates the actual computation to numeric_poly_stddev_internal with parameters indicating that this is a variance (not standard deviation) calculation for the entire population (not a sample).

## Parameters / Member Variables
- : Function call information structure containing the aggregation state as argument 0

## Dependencies
- Functions called/Symbols referenced:
  - [numeric_poly_stddev_internal](numeric_poly_stddev_internal.md) (when HAVE_INT128 is defined)
  - [numeric_var_pop](numeric_var_pop.md) (fallback when HAVE_INT128 is not defined)
  - PG_ARGISNULL
  - PG_GETARG_POINTER
  - PG_RETURN_NULL
  - PG_RETURN_NUMERIC
- Called from:
  - No direct references found (likely called through PostgreSQL's function manager)

## Notes and Other Information
- This function is part of PostgreSQL's optimized numeric aggregation system introduced to handle large datasets more efficiently
- The conditional compilation (#ifdef HAVE_INT128) allows PostgreSQL to use the most efficient implementation available on the target platform
- Population variance calculation (as opposed to sample variance) uses the entire dataset without Bessel's correction
- Located in src/backend/utils/adt/numeric.c:6462-6482