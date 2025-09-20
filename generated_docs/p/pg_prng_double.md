# pg_prng_double

## Location
[src/common/pg_prng.c:268-289](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/pg_prng.c#L268-L289)

## Overview
Generates a random double-precision floating-point number uniformly distributed in the range [0.0, 1.0).

## Definition

```c
double
pg_prng_double(pg_prng_state *state)
```
## Detailed Description
This function selects a random double uniformly from the half-open interval [0.0, 1.0). The implementation extracts the upper 52 bits from the 64-bit xoroshiro128** output (matching the mantissa precision of IEEE 754 double-precision format) and uses ldexp() to scale it to the [0.0, 1.0) range. The function assumes IEEE 754 double arithmetic, which is standard in PostgreSQL.

For the range (0.0, 1.0], the standard approach is to compute "1.0 - pg_prng_double(state)".

## Parameters / Member Variables
- : Pointer to the pseudo-random number generator state structure

## Dependencies
- Functions called/Symbols referenced:
  - xoroshiro128ss (the core PRNG algorithm)
  - ldexp (scales a floating-point value by a power of 2)
  - pg_prng_state (state structure type)
- Called from (representative examples):
  - gin_rand (GIN index random operations)
  - [StartTransaction](../S/StartTransaction.md) (transaction processing)
  - [geqo_rand](../g/geqo_rand.md) (genetic algorithm optimizer)
  - perform_spin_delay (lock contention handling)
  - [drandom](../d/drandom.md) (SQL random() function)
  - [sampler_random_fract](../s/sampler_random_fract.md) (sampling operations)
  - [getExponentialRand](../g/getExponentialRand.md), getPoissonRand (pgbench statistical distributions)
  - [pg_prng_double_normal](pg_prng_double_normal.md) (normal distribution generator)

## Notes and Other Information
- Uses upper 52 bits (v >> (64 - 52)) to match double precision mantissa
- [Result](../R/Result.md) could theoretically round to 1.0 if double precision is less than 52 bits, but PostgreSQL assumes IEEE 754
- Extensively used throughout PostgreSQL for statistical operations, sampling, and randomized algorithms
- Core building block for more complex probability distributions
- Part of PostgreSQL's unified PRNG interface for consistent random number generation
- Range is [0.0, 1.0) - includes 0.0 but excludes 1.0