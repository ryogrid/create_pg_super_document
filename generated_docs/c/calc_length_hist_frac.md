# calc_length_hist_frac

## Location
[src/backend/utils/adt/multirangetypes_selfuncs.c:966-1130](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes_selfuncs.c#L966-L1130)

## Overview
Calculates the average fraction of tuples with length < x (or <= x) in a given length interval using histogram data for multirange type selectivity estimation.

## Definition


## Detailed Description
This function computes the average of function P(x) over the interval [length1, length2], where P(x) represents the cumulative fraction of tuples with length < x (or <= x when equal=true). The calculation uses a piecewise integration approach through the length histogram bins, treating each bin as a trapezoid and computing the area under the curve.

The function implements the mathematical formula:


Where A=length1 and B=length2. The geometrical interpretation is calculating the area under the graph of P(x) defined by the length histogram.

## Parameters / Member Variables
- : Array of histogram bin boundary values for range lengths
- : Number of values in the length histogram array  
- : Lower bound of the length interval to analyze
- : Upper bound of the length interval to analyze
- : If true, use <= comparison instead of < for boundary conditions

## Dependencies
- Functions called/Symbols referenced:
  - [length_hist_bsearch](../l/length_hist_bsearch.md)
  - [get_len_position](../g/get_len_position.md)
  - [DatumGetFloat8](../D/DatumGetFloat8.md)
  - isinf
- Called from (representative examples):
  - [calc_hist_selectivity_contained](calc_hist_selectivity_contained.md)
  - [calc_hist_selectivity_contains](calc_hist_selectivity_contains.md)

## Notes and Other Information
- Uses binary search to find the appropriate histogram bin containing the bounds
- Handles edge cases like infinite lengths and zero-width intervals gracefully
- Returns 0.5 for infinite/infinite cases to avoid NaN
- Critical component in PostgreSQL's range type selectivity estimation system
- Operates on the assumption that range lengths follow the distribution captured in the histogram