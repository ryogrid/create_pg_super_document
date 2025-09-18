# setup_pct_info

## Location
src/backend/utils/adt/orderedsetaggs.c: 662 - 730

## Overview
A static function that constructs an array of pct_info structures showing which rows to sample for percentile calculations in ordered set aggregates.

## Definition
```c
static struct pct_info *setup_pct_info(int num_percentiles,
                                      Datum *percentiles_datum,
                                      bool *percentiles_null,
                                      int64 rowcount,
                                      bool continuous)
```

## Detailed Description
This function prepares the necessary metadata for percentile calculations by processing an array of percentile values and determining which specific rows need to be accessed from the sorted input data. It handles both continuous and discrete percentile calculations with different algorithms.

For continuous percentiles (percentile_cont), it calculates interpolation between adjacent rows by determining first_row, second_row, and the proportion for linear interpolation. For discrete percentiles (percentile_disc), it identifies the single row that represents the percentile value.

The function validates percentile values to ensure they are between 0 and 1, handles NULL percentile values as dummy entries, and sorts the resulting pct_info array by row positions to optimize data access patterns.

## Parameters / Member Variables
- `num_percentiles`: Number of percentile values to process
- `percentiles_datum`: Array of percentile values as Datum objects
- `percentiles_null`: Boolean array indicating which percentile values are NULL  
- `rowcount`: Total number of rows in the sorted input data
- `continuous`: Boolean flag indicating whether to use continuous (true) or discrete (false) percentile calculation

## Dependencies
- Functions called/Symbols referenced:
  - pct_info (struct type)
  - DatumGetFloat8
  - isnan
  - ereport/errcode/errmsg (error reporting)
  - palloc (memory allocation)
  - floor/ceil (math functions)
  - Max (macro)
  - qsort
  - pct_info_cmp
- Called from (representative examples):
  - percentile_disc_multi_final
  - percentile_cont_multi_final_common

## Notes and Other Information
- This is a static function with internal linkage within orderedsetaggs.c
- Validates percentile values are in valid range [0,1] and not NaN, throwing errors for invalid values
- Uses different algorithms for continuous vs discrete percentiles:
  - Continuous: Calculates interpolation between adjacent rows using floor/ceil
  - Discrete: Finds the smallest K where (K/N) >= percentile using ceiling function
- Sorts the output array by row positions to optimize sequential data access during percentile calculation
- Handles NULL percentile values by creating dummy entries with zero values
- Memory for the pct_info array is allocated using palloc and should be freed by the caller