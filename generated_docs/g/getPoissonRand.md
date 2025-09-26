# getPoissonRand

## Location
[src/bin/pgbench/pgbench.c:1179-1200](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L1179-L1200)

## Overview
Generates random integers following an approximate Poisson distribution centered on a given value using inverse transform sampling.

## Definition

```c
static int64
getPoissonRand(pg_prng_state *state, double center)
```
## Detailed Description
This function implements a random number generator that produces values approximating a Poisson distribution. It uses inverse transform sampling with the natural logarithm to transform a uniform random variable into a Poisson-distributed one. The function generates individual integer results, though the center parameter need not be an integer. The implementation converts a uniform random value from [0,1) to (0,1] and applies the inverse transform formula: -ln(uniform) * center, then rounds to the nearest integer.

## Parameters / Member Variables
- : Pointer to the pseudo-random number generator state used for generating uniform random values
- : The center (mean) value of the desired Poisson distribution; the expected average of generated values

## Dependencies
- Functions called/Symbols referenced:
  - [pg_prng_double](../p/pg_prng_double.md)
  - [pg_prng_state](../p/pg_prng_state.md) (type)
- Called from (representative examples):
  - [advanceConnectionState](../a/advanceConnectionState.md)

## Notes and Other Information
- Uses inverse transform sampling method for Poisson distribution generation
- Results are rounded to integers using standard rounding (+ 0.5)
- The function ensures the uniform random value is in (0,1] by computing 1.0 - [pg_prng_double](../p/pg_prng_double.md)(state)
- Part of pgbench utility for PostgreSQL performance testing
- Located in src/bin/pgbench/pgbench.c:1179-1200