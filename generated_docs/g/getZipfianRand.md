# getZipfianRand

## Location
src/bin/pgbench/pgbench.c: 1231 - 1244

## Overview
Generates random integers following a Zipfian distribution within a specified range [min, max] inclusive.

## Definition
```c
static int64 getZipfianRand(pg_prng_state *state, int64 min, int64 max, double s)
```

## Detailed Description
This function serves as a wrapper around computeIterativeZipfian to generate Zipfian-distributed random numbers within a user-specified range. It validates that the shape parameter s falls within acceptable bounds (MIN_ZIPFIAN_PARAM to MAX_ZIPFIAN_PARAM) and transforms the output from computeIterativeZipfian to fit the desired range. The function calculates the range size n = max - min + 1, generates a Zipfian-distributed value in [1, n], then adjusts it to the target range [min, max].

## Parameters / Member Variables
- `state`: Pointer to the pseudo-random number generator state for generating uniform random values
- `min`: Lower bound of the desired range (inclusive)
- `max`: Upper bound of the desired range (inclusive); must be >= min
- `s`: Shape parameter of the Zipfian distribution; must be within [MIN_ZIPFIAN_PARAM, MAX_ZIPFIAN_PARAM]

## Dependencies
- Functions called/Symbols referenced:
  - computeIterativeZipfian
  - pg_prng_state (type)
  - MIN_ZIPFIAN_PARAM (constant)
  - MAX_ZIPFIAN_PARAM (constant)
  - Assert (macro)
- Called from (representative examples):
  - evalStandardFunc

## Notes and Other Information
- Validates shape parameter bounds using Assert macro
- Transforms the [1, n] output from computeIterativeZipfian to [min, max] range
- Used in pgbench for generating realistic non-uniform data access patterns
- Part of pgbench's standard function evaluation system
- Located in src/bin/pgbench/pgbench.c:1231-1244