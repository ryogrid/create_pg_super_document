# computeIterativeZipfian

## Location
src/bin/pgbench/pgbench.c: 1201 - 1230

## Overview
Implements a Zipfian random number generator using the rejection method for generating non-uniform random variates with parameter s > 1.0.

## Definition
```c
static int64 computeIterativeZipfian(pg_prng_state *state, int64 n, double s)
```

## Detailed Description
This function generates random integers following a Zipfian distribution using the rejection method as described in "Non-Uniform Random Variate Generation" by Luc Devroye (p. 550-551, Springer 1986). The algorithm works by repeatedly generating candidate values using inverse transform sampling until one passes the acceptance condition. The method is effective for s > 1.0 but may perform poorly when s is very close to 1.0. The function ensures that generated values are within the range [1, n].

## Parameters / Member Variables
- `state`: Pointer to the pseudo-random number generator state for generating uniform random values
- `n`: Upper bound for the generated random values; must be > 1 for meaningful distribution
- `s`: Shape parameter of the Zipfian distribution; must be > 1.0 for this implementation

## Dependencies
- Functions called/Symbols referenced:
  - pg_prng_double
  - pg_prng_state (type)
  - pow (math function)
  - floor (math function)
- Called from (representative examples):
  - getZipfianRand

## Notes and Other Information
- Uses rejection sampling method which may require multiple iterations
- Performance degrades when s approaches 1.0
- Returns 1 immediately if n <= 1 (degenerate case)
- Based on established mathematical literature for non-uniform random variate generation
- Part of pgbench utility for PostgreSQL performance testing with realistic data distributions
- Located in src/bin/pgbench/pgbench.c:1201-1230