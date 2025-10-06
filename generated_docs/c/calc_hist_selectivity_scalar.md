# calc_hist_selectivity_scalar

## Location
[src/backend/utils/adt/multirangetypes_selfuncs.c:702-726](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes_selfuncs.c#L702-L726)

## Overview
Looks up the fraction of values less than (or equal to) a given range bound in a histogram of range bounds using binary search and linear interpolation.

## Definition
```c
static double calc_hist_selectivity_scalar(TypeCacheEntry *typcache, const RangeBound *constbound, const RangeBound *hist, int hist_nvalues, bool equal)
```

## Detailed Description
This function provides precise selectivity estimation for scalar comparisons against range bounds. It uses binary search to locate the appropriate histogram bin, then applies linear interpolation within that bin to get a more accurate estimate than just using whole bins.

The algorithm:
1. Uses rbound_bsearch() to find the histogram bin where the constant falls
2. Calculates base selectivity as the fraction of whole bins before the found bin
3. Applies linear interpolation within the bin using get_position() to refine the estimate
4. Handles edge cases for values outside the histogram range

## Parameters / Member Variables
- `typcache`: Type cache entry for range element type
- `constbound`: The constant range bound to compare against
- `hist`: Array of range bounds from the histogram
- `hist_nvalues`: Number of values in the histogram array
- `equal`: If true, include equal values in the "less than or equal" calculation

## Dependencies
- Functions called/Symbols referenced:
  - rbound_bsearch
  - get_position
  - Max macro

- Called from (representative examples):
  - calc_hist_selectivity (for various operator types)

## Notes and Other Information
This function is fundamental to multirange selectivity estimation, providing the mathematical foundation for histogram-based analysis. The linear interpolation step significantly improves accuracy over simple bin counting, especially for non-uniform data distributions.

## Simplified Source

```c
static double
calc_hist_selectivity_scalar(TypeCacheEntry *typcache, const RangeBound *constbound,
                           const RangeBound *hist, int hist_nvalues, bool equal)
{
    Selectivity selec;
    int index;

    // Find histogram bin using binary search
    index = rbound_bsearch(typcache, constbound, hist, hist_nvalues, equal);

    // Base selectivity: fraction of preceding whole bins
    selec = (Selectivity) (Max(index, 0)) / (Selectivity) (hist_nvalues - 1);

    // Linear interpolation within the bin for more accuracy
    if (index >= 0 && index < hist_nvalues - 1)
        selec += get_position(typcache, constbound, &hist[index], &hist[index + 1])
                 / (Selectivity) (hist_nvalues - 1);

    return selec;
}
```