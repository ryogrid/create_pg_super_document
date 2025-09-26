# reservoir_init_selection_state

## Location
src/backend/utils/misc/sampling.c: 133 - 146

## Overview
Initializes the reservoir sampling state by computing the initial W value required for Algorithm Z from Vitter's reservoir sampling algorithm.

## Definition

```c
void
reservoir_init_selection_state(ReservoirState rs, int n)
```
## Detailed Description
reservoir_init_selection_state initializes a ReservoirState structure for use with Vitter's Algorithm Z reservoir sampling method. This function is part of PostgreSQL's implementation of "Random sampling with a reservoir" by Jeffrey S. Vitter (ACM Trans. Math. Softw. 11, 1, Mar. 1985, Pages 37-57).

The function performs two key initialization steps: first, it sets up the random number generator state using a seed derived from the global PRNG, and second, it computes the initial value of W, which is a crucial state variable in Algorithm Z. The W value is calculated using the formula W = exp(-log(U)/n), where U is a uniform random value in (0,1) and n is the desired sample size.

Since reservoir sampling in PostgreSQL doesn't need to return repeatable results, the function uses a random seed from the global PRNG state rather than a deterministic seed.

## Parameters / Member Variables
- : Pointer to the ReservoirState structure to initialize
- : Desired sample size (reservoir capacity)

## Dependencies
- Functions called/Symbols referenced:
  - sampler_random_init_state (initializes random number generator with seed)
  - pg_prng_uint32 (generates random seed from global PRNG)
  - sampler_random_fract (generates uniform random fraction)
  - exp, log (mathematical functions for W calculation)
  - ReservoirStateData structure members (W, randstate)
- Called from (representative examples):
  - acquire_sample_rows (in src/backend/commands/analyze.c:1194)

## Notes and Other Information
- Implements initialization for Vitter's Algorithm Z, which is more efficient than Algorithm R for large datasets
- The W variable represents a random state that persists between calls to reservoir_get_next_S
- Uses non-deterministic random seeding since PostgreSQL's sampling doesn't require reproducibility
- The initial W computation follows the mathematical foundation of Algorithm Z for determining skip distances
- Must be called before using reservoir_get_next_S to ensure proper algorithm state
- Part of PostgreSQL's ANALYZE command infrastructure for statistical sampling when table size is unknown in advance