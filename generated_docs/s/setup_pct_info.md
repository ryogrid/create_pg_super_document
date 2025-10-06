# setup_pct_info

## Location
[src/backend/utils/adt/orderedsetaggs.c:662-730](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/orderedsetaggs.c#L662-L730)

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
  - [pct_info](../p/pct_info.md) (struct type)
  - [DatumGetFloat8](../D/DatumGetFloat8.md)
  - isnan
  - ereport/errcode/errmsg (error reporting)
  - [palloc](../p/palloc.md) (memory allocation)
  - floor/ceil (math functions)
  - Max (macro)
  - qsort
  - [pct_info_cmp](../p/pct_info_cmp.md)
- Called from (representative examples):
  - [percentile_disc_multi_final](../p/percentile_disc_multi_final.md)
  - [percentile_cont_multi_final_common](../p/percentile_cont_multi_final_common.md)

## Notes and Other Information
- This is a static function with internal linkage within orderedsetaggs.c
- Validates percentile values are in valid range [0,1] and not NaN, throwing errors for invalid values
- Uses different algorithms for continuous vs discrete percentiles:
  - Continuous: Calculates interpolation between adjacent rows using floor/ceil
  - Discrete: Finds the smallest K where (K/N) >= percentile using ceiling function
- Sorts the output array by row positions to optimize sequential data access during percentile calculation
- Handles NULL percentile values by creating dummy entries with zero values
- Memory for the pct_info array is allocated using palloc and should be freed by the caller

## Simplified Source

```c
static struct pct_info *
setup_pct_info(int num_percentiles,
               Datum *percentiles_datum,
               bool *percentiles_null,
               int64 rowcount,
               bool continuous)
{
    struct pct_info *pct_info;
    int i;

    // Allocate result array
    pct_info = (struct pct_info *) palloc(num_percentiles * sizeof(struct pct_info));

    for (i = 0; i < num_percentiles; i++) {
        pct_info[i].idx = i;

        if (percentiles_null[i]) {
            // Create dummy entry for NULL percentiles
            pct_info[i].first_row = 0;
            pct_info[i].second_row = 0;
            pct_info[i].proportion = 0;
        } else {
            double p = DatumGetFloat8(percentiles_datum[i]);

            // Validate percentile range
            if (p < 0 || p > 1 || isnan(p))
                ereport(ERROR, "percentile value must be between 0 and 1");

            if (continuous) {
                // Continuous percentiles: calculate interpolation bounds
                pct_info[i].first_row = 1 + floor(p * (rowcount - 1));
                pct_info[i].second_row = 1 + ceil(p * (rowcount - 1));
                pct_info[i].proportion = (p * (rowcount - 1)) - floor(p * (rowcount - 1));
            } else {
                // Discrete percentiles: find smallest K where (K/N) >= percentile
                int64 row = (int64) ceil(p * rowcount);
                row = Max(1, row);
                pct_info[i].first_row = row;
                pct_info[i].second_row = row;
                pct_info[i].proportion = 0;
            }
        }
    }

    // Sort by row positions for optimal access
    qsort(pct_info, num_percentiles, sizeof(struct pct_info), pct_info_cmp);

    return pct_info;
}
```