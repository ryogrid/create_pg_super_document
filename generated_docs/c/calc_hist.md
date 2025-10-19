# calc_hist

## Location
[src/backend/utils/adt/array_selfuncs.c:921-1009](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_selfuncs.c#L921-L1009)

## Overview
Calculates probability distribution for the first n distinct element counts from a histogram of distinct element counts.

## Definition
```c
static float *calc_hist(const float4 *hist, int nhist, int n)
```

## Detailed Description
This function converts a histogram of distinct element counts into a probability array where each entry represents the probability of having exactly k distinct elements (for k in [0..n]). It handles histogram data by assuming that each histogram box with bounds a and b distributes probability uniformly across the range, with edge values receiving additional weight.

The algorithm processes histogram boundaries and calculates probability contributions based on the number of intervals (nhist - 1) between histogram values. For values that appear as exact histogram boundaries, it accounts for both exclusive containment in histogram boxes and partial contributions from adjacent boxes.

## Parameters / Member Variables
- `hist`: Array of histogram boundary values representing distinct element counts
- `nhist`: Number of histogram boundaries
- `n`: Maximum element count to calculate probabilities for

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md)
  - float4
- Called from (representative examples):
  - [mcelem_array_contained_selec](../m/mcelem_array_contained_selec.md) (referenced via DEFAULT_SEL)
  - Functions in array selectivity estimation (referenced via EFFORT)

## Notes and Other Information
- Returns a palloc'd array of (n+1) float entries, with array[k] = probability of k distinct elements
- Assumes histogram values are theoretically integers but handles potential floating-point roundoff errors
- Uses uniform distribution assumption within histogram intervals: 1/((b-a+1)*(nhist-1)) per value
- [Edge](../E/Edge.md) values (histogram boundaries) receive additional probability weight of 0.5/interval_length
- Each interval between histogram values contributes frac = 1.0/(nhist-1) total probability
- Handles cases where k does not appear as an exact histogram boundary by using interval-based calculation

## Simplified Source

```c
static float *
calc_hist(const float4 *hist, int nhist, int n)
{
    // Allocate result array for probabilities [0..n]
    float *hist_part = (float *) palloc((n + 1) * sizeof(float));

    // Each histogram interval contributes equal probability
    float frac = 1.0f / (float)(nhist - 1);

    int i = 0;  // Current histogram index
    float prev_interval = 0, next_interval;

    // Calculate probability for each distinct element count k
    for (int k = 0; k <= n; k++)
    {
        int count = 0;

        // Count histogram boundaries that equal k
        while (i < nhist && hist[i] <= k)
        {
            count++;
            i++;
        }

        if (count > 0)
        {
            // k appears as exact histogram boundary
            // Calculate interval to next boundary
            if (i < nhist)
                next_interval = hist[i] - hist[i - 1];
            else
                next_interval = 0;

            // Probability from exclusive boxes plus edge contributions
            float val = (float)(count - 1);
            if (next_interval > 0)
                val += 0.5f / next_interval;  // Right edge contribution
            if (prev_interval > 0)
                val += 0.5f / prev_interval;  // Left edge contribution

            hist_part[k] = frac * val;
            prev_interval = next_interval;
        }
        else
        {
            // k is not an exact boundary, interpolate from interval
            if (prev_interval > 0)
                hist_part[k] = frac / prev_interval;
            else
                hist_part[k] = 0.0f;
        }
    }

    return hist_part;
}
```