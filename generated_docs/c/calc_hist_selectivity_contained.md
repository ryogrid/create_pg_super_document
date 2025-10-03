# calc_hist_selectivity_contained

## Location
[src/backend/utils/adt/multirangetypes_selfuncs.c:1131-1251](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes_selfuncs.c#L1131-L1251)

## Overview
Calculates selectivity of the "var <@ const" operator, estimating the fraction of multiranges that fall within constant lower and upper bounds using histograms.

## Definition

```c
static double
calc_hist_selectivity_contained(TypeCacheEntry *typcache,
								const RangeBound *lower, RangeBound *upper,
								const RangeBound *hist_lower, int hist_nvalues,
								Datum *length_hist_values, int length_hist_nvalues)
```
## Detailed Description
This function estimates what fraction of multiranges in the database are contained within (i.e., fall completely inside) a given constant range. It uses two histograms: one for range lower bounds and one for range lengths. The core assumption is that range lengths are independent of the lower bounds, allowing separate analysis of each component.

The algorithm works by:
1. Finding the relevant bins in the lower bound histogram (ranges with lower bounds > constant upper can't match)
2. For each relevant bin, calculating what fraction of ranges would be narrow enough to fit within the constant range
3. Summing these fractions weighted by the bin populations

## Parameters / Member Variables
- `*typcache`: Type cache entry containing range type information and comparison functions
- `*lower`: Lower bound of the constant range for containment testing
- `*upper`: Upper bound of the constant range for containment testing
- `*hist_lower`: Array of histogram values for range lower bounds
- `hist_nvalues`: Number of values in the lower bound histogram
- `*length_hist_values`: Array of histogram values for range lengths
- `length_hist_nvalues`: Number of values in the length histogram
## Dependencies
- Functions called/Symbols referenced:
  - [rbound_bsearch](../r/rbound_bsearch.md)
  - [get_position](../g/get_position.md)
  - [range_cmp_bounds](../r/range_cmp_bounds.md)
  - [get_distance](../g/get_distance.md)
  - [calc_length_hist_frac](calc_length_hist_frac.md)
  - RangeBound
- Called from (representative examples):
  - [calc_hist_selectivity](calc_hist_selectivity.md)

## Notes and Other Information
- Implements selectivity estimation for the contained-by operator (<@) on range types
- Uses linear interpolation for precise bin boundary calculations
- Handles edge cases where bounds fall outside histogram limits
- Critical for query optimization when filtering ranges by containment conditions
- Assumes independence between range start positions and lengths, which is generally reasonable for most real-world data