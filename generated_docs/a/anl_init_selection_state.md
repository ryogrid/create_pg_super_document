# anl_init_selection_state

## Location
src/backend/utils/misc/sampling.c: 281 - 295

## Overview
Initializes the selection state for Algorithm Z (Vitter's reservoir sampling algorithm) by computing the initial W value used in ANALYZE operations.

## Definition
```c
double anl_init_selection_state(int n)
```

## Detailed Description
This function implements the initialization step for Vitter's Algorithm Z, which is an efficient method for reservoir sampling. It computes the initial value of W, which represents the probability multiplier used to determine how many records to skip before selecting the next sample. The function uses the formula W = exp(-log(U)/n) where U is a random value in (0,1) and n is the reservoir size. Like anl_random_fract(), it manages its own global random state with lazy initialization, ensuring consistent random behavior across ANALYZE operations.

## Parameters / Member Variables
- `n`: The size of the reservoir (number of samples to maintain), used to compute the initial W value for Algorithm Z

## Dependencies
- Functions called/Symbols referenced:
  - [sampler_random_init_state](../s/sampler_random_init_state.md)
  - [pg_prng_uint32](../p/pg_prng_uint32.md)
  - [sampler_random_fract](../s/sampler_random_fract.md)
  - exp (math library function)
  - log (math library function)
- Called from (representative examples):
  - Referenced in MAX_STATISTICS_TARGET context
  - Used in ReservoirState context

## Notes and Other Information
This function is part of PostgreSQL's implementation of Vitter's Algorithm Z for efficient reservoir sampling during table analysis. The W value it computes determines the skip length for the next iteration of the algorithm, allowing for more efficient sampling of large datasets. The mathematical formula exp(-log(U)/n) is mathematically equivalent to U^(1/n) but is computed using exp and log functions for numerical stability. The lazy initialization pattern ensures the random state is only set up when needed, sharing the same global state management as anl_random_fract().