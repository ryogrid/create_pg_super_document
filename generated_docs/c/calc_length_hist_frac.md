# calc_length_hist_frac

## Location
[src/backend/utils/adt/multirangetypes_selfuncs.c:966-1130](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes_selfuncs.c#L966-L1130)

## Overview
Calculates the average fraction of tuples with length < x (or <= x) in a given length interval using histogram data for multirange type selectivity estimation.

## Definition

```c
static double
calc_length_hist_frac(Datum *length_hist_values, int length_hist_nvalues,
					  double length1, double length2, bool equal)
```
## Detailed Description
This function computes the average of function P(x) over the interval [length1, length2], where P(x) represents the cumulative fraction of tuples with length < x (or <= x when equal=true). The calculation uses a piecewise integration approach through the length histogram bins, treating each bin as a trapezoid and computing the area under the curve.

The function implements the mathematical formula:


Where A=length1 and B=length2. The geometrical interpretation is calculating the area under the graph of P(x) defined by the length histogram.

## Parameters / Member Variables
- `*length_hist_values`: Array of histogram bin boundary values for range lengths
- `length_hist_nvalues`: Number of values in the length histogram array
- `length1`: Lower bound of the length interval to analyze
- `length2`: Upper bound of the length interval to analyze
- `equal`: If true, use <= comparison instead of < for boundary conditions
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

## Simplified Source

```c
static double
calc_length_hist_frac(Datum *length_hist_values, int length_hist_nvalues,
                      double length1, double length2, bool equal)
{
    double frac, area, pos;
    double A, B, PA, PB;
    int i;

    // Basic bounds checking
    if (length2 < 0.0) return 0.0;
    if (isinf(length2) && equal) return 1.0;

    // Find starting histogram bin using binary search
    i = length_hist_bsearch(length_hist_values, length_hist_nvalues, length1, equal);
    if (i >= length_hist_nvalues - 1) return 1.0;

    // Calculate initial position and probability
    if (i < 0) {
        i = 0;
        pos = 0.0;
    } else {
        pos = get_len_position(length1,
                              DatumGetFloat8(length_hist_values[i]),
                              DatumGetFloat8(length_hist_values[i + 1]));
    }
    PB = (((double) i) + pos) / (double) (length_hist_nvalues - 1);
    B = length1;

    // Handle degenerate case where length1 == length2
    if (length2 == length1) return PB;

    // Integrate through histogram bins (trapezoid area calculation)
    area = 0.0;
    for (; i < length_hist_nvalues - 1; i++) {
        double bin_upper = DatumGetFloat8(length_hist_values[i + 1]);

        // Check if we've reached the last bin
        if (!(bin_upper < length2 || (equal && bin_upper <= length2)))
            break;

        // Update bounds for this trapezoid
        A = B;
        PA = PB;
        B = bin_upper;
        PB = (double) i / (double) (length_hist_nvalues - 1);

        // Add trapezoid area: 0.5 * (height1 + height2) * width
        if (PA > 0 || PB > 0)
            area += 0.5 * (PB + PA) * (B - A);
    }

    // Handle final bin to upper bound
    A = B;
    PA = PB;
    B = length2;

    // Calculate final position and probability
    if (i >= length_hist_nvalues - 1) {
        pos = 0.0;
    } else {
        pos = get_len_position(length2,
                              DatumGetFloat8(length_hist_values[i]),
                              DatumGetFloat8(length_hist_values[i + 1]));
    }
    PB = (((double) i) + pos) / (double) (length_hist_nvalues - 1);

    if (PA > 0 || PB > 0)
        area += 0.5 * (PB + PA) * (B - A);

    // Calculate average: area / width, handle infinite cases
    if (isinf(area) && isinf(length2))
        frac = 0.5;
    else
        frac = area / (length2 - length1);

    return frac;
}
```