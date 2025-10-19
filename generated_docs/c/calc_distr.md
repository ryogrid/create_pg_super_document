# calc_distr

## Location
[src/backend/utils/adt/array_selfuncs.c:1010-1088](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_selfuncs.c#L1010-L1088)

## Overview
Calculates probability distribution for exact k occurrences of n independent events with given probabilities, including rare events modeled with Poisson distribution.

## Definition
```c
static float *calc_distr(const float *p, int n, int m, float rest)
```

## Detailed Description
This function implements a dynamic programming algorithm to compute the probability distribution of exactly k events occurring out of n independent events with known probabilities p[]. It constructs a matrix M where M[i,j] represents the probability that exactly j of the first i events occur.

The algorithm uses the recurrence relation:
- M[i,j] = M[i-1,j] * (1-p[i]) + M[i-1,j-1] * p[i] for i > 0, j > 0
- M[i,0] = M[i-1,0] * (1-p[i]) for i > 0

Additionally, it models the collective effect of rare elements (not included in p[]) using the Poisson distribution, convolving the computed distribution with a Poisson distribution having parameter "rest".

## Parameters / Member Variables
- `p`: Array of event probabilities for the n most significant events
- `n`: Number of events with known probabilities in array p[]
- `m`: Maximum number of occurrences to calculate probabilities for  
- `rest`: Sum of probabilities of all low-probability events not included in p[]

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md)
  - [pfree](../p/pfree.md)
  - exp (math function)
  - DEFAULT_CONTAIN_SEL
- Called from (representative examples):
  - [mcelem_array_contained_selec](../m/mcelem_array_contained_selec.md) (referenced via DEFAULT_SEL and EFFORT)
  - Array selectivity estimation functions

## Notes and Other Information
- Returns a palloc'd array of size (m+1) with probabilities for k=0 to k=m occurrences
- Uses only two rows of the full matrix to optimize memory usage during computation
- Applies Poisson distribution modeling when rest > DEFAULT_CONTAIN_SEL to account for rare elements
- The Poisson convolution models the scenario where many low-probability elements collectively affect selectivity
- Implements the law of total probability for independent events
- Space complexity: O(m), Time complexity: O(n*m) for the main calculation plus O(m²) for Poisson convolution

## Simplified Source

```c
static float *
calc_distr(const float *p, int n, int m, float rest)
{
    // Allocate two rows for dynamic programming matrix
    float *row = (float *) palloc((m + 1) * sizeof(float));
    float *prev_row = (float *) palloc((m + 1) * sizeof(float));

    // Initialize: M[0,0] = 1 (probability of 0 events from 0 events)
    row[0] = 1.0f;

    // Dynamic programming: calculate probabilities for each event
    for (int i = 1; i <= n; i++)
    {
        float prob = p[i - 1];  // Probability of current event

        // Swap current and previous rows
        float *tmp = row;
        row = prev_row;
        prev_row = tmp;

        // Calculate probabilities for j occurrences
        for (int j = 0; j <= i && j <= m; j++)
        {
            float val = 0.0f;

            // Probability that event doesn't occur
            if (j < i)
                val += prev_row[j] * (1.0f - prob);

            // Probability that event occurs
            if (j > 0)
                val += prev_row[j - 1] * prob;

            row[j] = val;
        }
    }

    // Handle rare elements using Poisson distribution
    if (rest > DEFAULT_CONTAIN_SEL)
    {
        // Swap rows for convolution calculation
        float *tmp = row;
        row = prev_row;
        prev_row = tmp;

        // Initialize result array
        for (int i = 0; i <= m; i++)
            row[i] = 0.0f;

        // Calculate Poisson probabilities and convolve
        float poisson_prob = exp(-rest);  // P(X=0) for Poisson(rest)

        for (int i = 0; i <= m; i++)
        {
            // Add contribution from Poisson value i
            for (int j = 0; j <= m - i; j++)
                row[j + i] += prev_row[j] * poisson_prob;

            // Update to next Poisson probability: P(X=i+1)
            poisson_prob *= rest / (float)(i + 1);
        }
    }

    pfree(prev_row);
    return row;
}
```