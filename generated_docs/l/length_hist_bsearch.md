# length_hist_bsearch

## Location
src/backend/utils/adt/multirangetypes_selfuncs.c: 768 - 793

## Overview
Binary search function that finds the greatest index in a length histogram array where the stored length is less than (or less than or equal to) a given target length value, used for range length selectivity estimation.

## Definition
```c
static int length_hist_bsearch(Datum *length_hist_values, int length_hist_nvalues, double value, bool equal)
```

## Detailed Description
This function performs a binary search on a histogram of range lengths stored as Datum values (converted to float8). It is specifically designed to support selectivity estimation for range length-based queries. The function helps determine which histogram bin a given length value falls into, enabling interpolation calculations for estimating the selectivity of length-based predicates on range types.

The search operates on float8 values extracted from Datum array elements using `DatumGetFloat8()`. When the `equal` flag is set, the comparison includes equality in the condition, otherwise it uses strict inequality.

## Parameters / Member Variables
- `length_hist_values`: Array of Datum values representing length histogram bin boundaries (stored as float8)
- `length_hist_nvalues`: Number of elements in the length histogram array
- `value`: Target length value to search for in the histogram (as double)
- `equal`: Flag determining whether to include equality in the comparison (≤ vs <)

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetFloat8](../D/DatumGetFloat8.md)
- Called from (representative examples):
  - [calc_length_hist_frac](../c/calc_length_hist_frac.md)

## Notes and Other Information
- Returns -1 if all histogram lengths are greater than (or greater than or equal to) the target value
- Used in both regular range types and multirange types length-based selectivity calculations
- The histogram values are stored as Datum but treated as float8 representing length measurements
- Essential for PostgreSQL query planner's cost estimation when dealing with range length predicates