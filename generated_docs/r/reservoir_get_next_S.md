# reservoir_get_next_S

## Location
[src/backend/utils/misc/sampling.c:147-233](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/sampling.c#L147-L233)

## Overview
Computes the number of records to skip before selecting the next record in Vitter's Algorithm Z reservoir sampling implementation.

## Definition

```c
double
reservoir_get_next_S(ReservoirState rs, double t, int n)
```
## Detailed Description
reservoir_get_next_S implements the core logic of Vitter's Algorithm Z for reservoir sampling. The function determines S, the count of records to skip before processing the next record, based on the current number of records read (t) and the desired sample size (n).

The algorithm uses a two-phase approach: when t <= 22*n, it employs Algorithm X, which is simpler but less efficient for large datasets. For larger values of t, it switches to the more sophisticated Algorithm Z. The magic constant 22 comes from Vitter's analysis of when Algorithm Z becomes more efficient than Algorithm X.

Algorithm X uses a straightforward approach, generating random values and incrementally computing S until the acceptance condition (4.1) from Vitter's paper is satisfied. Algorithm Z uses more complex mathematical computations involving acceptance/rejection testing with sophisticated probability calculations to achieve better performance for large datasets.

## Parameters / Member Variables
- : Pointer to the ReservoirState structure containing the random state and W value
- : Number of records already processed (must be >= n for reservoir sampling)
- : Desired sample size (reservoir capacity)

## Dependencies
- Functions called/Symbols referenced:
  - [sampler_random_fract](../s/sampler_random_fract.md) (generates uniform random fractions)
  - floor, exp, log (mathematical functions for Algorithm Z calculations)
  - [ReservoirStateData](../R/ReservoirStateData.md) structure members (W, randstate)
- Called from (representative examples):
  - [acquire_sample_rows](../a/acquire_sample_rows.md) (in src/backend/commands/analyze.c:1236)
  - [anl_get_next_S](../a/anl_get_next_S.md) (backward compatibility wrapper)

## Notes and Other Information
- Implements both Algorithm X (for t <= 22*n) and Algorithm Z (for t > 22*n) from Vitter's paper
- The threshold value 22 is Vitter's empirically determined crossover point for algorithm efficiency
- Algorithm Z maintains the W state variable across calls for efficiency, updating it when records are selected
- Returns a skip count S that indicates how many records to skip before selecting the next one
- The complex mathematical computations in Algorithm Z implement acceptance/rejection testing for optimal performance
- Used in PostgreSQL's ANALYZE command when the total number of table records is unknown in advance
- More efficient than simple random sampling for large datasets due to reduced random number generation overhead