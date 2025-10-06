# calc_hist_selectivity_contains

## Location
[src/backend/utils/adt/multirangetypes_selfuncs.c:1252-1336](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes_selfuncs.c#L1252-L1336)

## Overview
Calculates selectivity of the "var @> const" operator, estimating the fraction of multiranges that contain the constant lower and upper bounds using histograms.

## Definition

```c
static double
calc_hist_selectivity_contains(TypeCacheEntry *typcache,
							   const RangeBound *lower, const RangeBound *upper,
							   const RangeBound *hist_lower, int hist_nvalues,
							   Datum *length_hist_values, int length_hist_nvalues)
```
## Detailed Description
This function estimates what fraction of multiranges in the database contain (i.e., completely encompass) a given constant range. Like its counterpart calc_hist_selectivity_contained, it uses histograms of range lower bounds and lengths, assuming independence between these properties.

The algorithm works by:
1. Finding the bin containing the lower bound of the query range in the lower bound histogram
2. Walking backwards through bins with lower bounds <= query lower bound
3. For each bin, calculating what fraction of ranges would be long enough to extend past the query upper bound
4. Summing these fractions weighted by bin populations

This is essentially the complement operation to containment - instead of asking "how many ranges fit inside this constant range?", it asks "how many ranges does this constant range fit inside?"

## Parameters / Member Variables
- `*typcache`: Type cache entry containing range type information and comparison functions
- `*lower`: Lower bound of the constant range for contains testing
- `*upper`: Upper bound of the constant range for contains testing
- `*hist_lower`: Array of histogram values for range lower bounds
- `hist_nvalues`: Number of values in the lower bound histogram
- `*length_hist_values`: Array of histogram values for range lengths
- `length_hist_nvalues`: Number of values in the length histogram
## Dependencies
- Functions called/Symbols referenced:
  - [rbound_bsearch](../r/rbound_bsearch.md)
  - [get_position](../g/get_position.md)
  - [get_distance](../g/get_distance.md)
  - [calc_length_hist_frac](calc_length_hist_frac.md)
  - RangeBound
- Called from (representative examples):
  - [calc_hist_selectivity](calc_hist_selectivity.md)

## Notes and Other Information
- Implements selectivity estimation for the contains operator (@>) on range types
- Uses complement of calc_length_hist_frac (1.0 - result) to find ranges long enough to contain the query
- Critical for optimizing queries that filter by range containment relationships
- Handles boundary conditions and edge cases similar to calc_hist_selectivity_contained
- Essential component of PostgreSQL's cost-based optimizer for range queries

## Simplified Source

```c
static double
calc_hist_selectivity_contains(TypeCacheEntry *typcache,
                              const RangeBound *lower, const RangeBound *upper,
                              const RangeBound *hist_lower, int hist_nvalues,
                              Datum *length_hist_values, int length_hist_nvalues)
{
    int i, lower_index;
    double bin_width, lower_bin_width;
    double sum_frac;
    float8 prev_dist;

    // Find the bin containing the lower bound of query range
    lower_index = rbound_bsearch(typcache, lower, hist_lower, hist_nvalues, true);

    // No matches if lower bound is below histogram's lower limit
    if (lower_index < 0) return 0.0;

    // Clamp to last actual bin if beyond histogram's upper limit
    lower_index = Min(lower_index, hist_nvalues - 2);

    // Calculate fraction of lower bin that's greater than query lower bound
    lower_bin_width = get_position(typcache, lower, &hist_lower[lower_index],
                                  &hist_lower[lower_index + 1]);

    // Initialize: start from query lower bound and walk backwards
    // First distance is the length of the query range itself
    prev_dist = get_distance(typcache, lower, upper);
    sum_frac = 0.0;
    bin_width = lower_bin_width;

    // Process bins from lower_index down to 0
    for (i = lower_index; i >= 0; i--) {
        float8 dist;
        double length_hist_frac;

        // Distance from query upper bound to current histogram lower bound
        dist = get_distance(typcache, &hist_lower[i], upper);

        // Get fraction of length histogram with intervals >= distance
        // Use complement (1.0 - calc_length_hist_frac) to find ranges
        // long enough to contain the query range
        length_hist_frac = 1.0 - calc_length_hist_frac(length_hist_values,
                                                       length_hist_nvalues,
                                                       prev_dist, dist, false);

        // Add weighted fraction to total
        sum_frac += length_hist_frac * bin_width / (double) (hist_nvalues - 1);

        bin_width = 1.0;
        prev_dist = dist;
    }

    return sum_frac;
}
```