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

## Simplified Source

```c
static double
calc_hist_selectivity_contained(TypeCacheEntry *typcache,
                               const RangeBound *lower, RangeBound *upper,
                               const RangeBound *hist_lower, int hist_nvalues,
                               Datum *length_hist_values, int length_hist_nvalues)
{
    int i, upper_index;
    double bin_width, upper_bin_width;
    double sum_frac, prev_dist;

    // Find the bin containing the upper bound in lower bound histogram
    // Ranges with lower bound > constant upper bound can't match
    upper->inclusive = !upper->inclusive;
    upper->lower = true;
    upper_index = rbound_bsearch(typcache, upper, hist_lower, hist_nvalues, false);

    // No matches if upper bound is below histogram's lower limit
    if (upper_index < 0) return 0.0;

    // Clamp to last actual bin if beyond histogram's upper limit
    upper_index = Min(upper_index, hist_nvalues - 2);

    // Calculate fraction of upper bin that's greater than query upper bound
    upper_bin_width = get_position(typcache, upper,
                                  &hist_lower[upper_index],
                                  &hist_lower[upper_index + 1]);

    // Initialize for loop iteration
    prev_dist = 0.0;
    bin_width = upper_bin_width;
    sum_frac = 0.0;

    // Process histogram bins from upper_index down to 0
    for (i = upper_index; i >= 0; i--) {
        double dist, length_hist_frac;
        bool final_bin = false;

        // Calculate distance from query upper bound to current bin's lower bound
        if (range_cmp_bounds(typcache, &hist_lower[i], lower) < 0) {
            // This bin contains the constant lower bound (final bin)
            dist = get_distance(typcache, lower, upper);

            // Subtract portion of bin we want to ignore
            bin_width -= get_position(typcache, lower, &hist_lower[i],
                                    &hist_lower[i + 1]);
            if (bin_width < 0.0) bin_width = 0.0;
            final_bin = true;
        } else {
            dist = get_distance(typcache, &hist_lower[i], upper);
        }

        // Estimate fraction of tuples in this bin narrow enough to fit
        length_hist_frac = calc_length_hist_frac(length_hist_values,
                                                length_hist_nvalues,
                                                prev_dist, dist, true);

        // Add weighted fraction to total
        sum_frac += length_hist_frac * bin_width / (double) (hist_nvalues - 1);

        if (final_bin) break;

        bin_width = 1.0;
        prev_dist = dist;
    }

    return sum_frac;
}
```