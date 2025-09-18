# percentile_disc_multi_final

## Location
src/backend/utils/adt/orderedsetaggs.c: 731 - 847

## Overview
The final aggregate function that computes discrete percentiles for an array of percentile values using ordered set semantics.

## Definition
```c
Datum percentile_disc_multi_final(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the final phase of the percentile_disc aggregate function when multiple percentile values are requested simultaneously. It processes a sorted dataset and returns exact values from specific row positions that correspond to each requested percentile.

Unlike continuous percentiles which interpolate between values, discrete percentiles return the actual value at the computed row position. The function efficiently handles multiple percentiles by sorting the required row positions and scanning through the dataset only once.

The function handles various edge cases including NULL input values, empty datasets, NULL percentile values in the input array, and missing rows. It maintains the same array structure as the input percentile array for the output.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0: OSAPerGroupState pointer (aggregate state)  
  - Argument 1: ArrayType pointer (array of percentile values)

## Dependencies
- Functions called/Symbols referenced:
  - OSAPerGroupState (struct type)
  - pct_info (struct type)
  - AggCheckCallContext
  - PG_GETARG_ARRAYTYPE_P
  - deconstruct_array_builtin
  - construct_empty_array
  - setup_pct_info
  - tuplesort_performsort
  - tuplesort_rescan
  - tuplesort_skiptuples  
  - tuplesort_getdatum
  - construct_md_array
  - ARR_NDIM/ARR_DIMS/ARR_LBOUND (array macros)
  - palloc (memory allocation)
- Called from (representative examples):
  - PostgreSQL aggregate execution framework (no direct code references found)

## Notes and Other Information
- This is a PostgreSQL aggregate final function, called automatically by the executor during aggregate processing
- Uses tuplesort for efficient access to sorted data, avoiding full dataset materialization
- Optimizes performance by processing multiple percentiles in a single scan when possible
- Returns NULL if no input rows exist or if the percentile array argument is NULL
- Preserves the dimensionality and bounds of the input percentile array in the output
- Error handling includes checks for missing rows that should be present based on row count
- Memory management uses palloc for result arrays which will be cleaned up by PostgreSQL's memory context system
- Part of PostgreSQL's ordered-set aggregate framework for statistical functions