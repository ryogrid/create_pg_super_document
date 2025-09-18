# getGaussianRand

## Location
src/bin/pgbench/pgbench.c: 1137 - 1178

## Overview
Generates random integers following a Gaussian (normal) distribution within a specified inclusive range, using a parameter to control the distribution width.

## Definition


## Detailed Description
The  function implements a Gaussian (normal) probability distribution for generating random integers within the range [min, max]. The function uses a rejection sampling approach to ensure that the generated normal random values fall within the specified parameter bounds before mapping them to the target integer range.

The implementation uses PostgreSQL's  function to generate normally-distributed random numbers, then applies rejection sampling to constrain the standard deviation to the range [-parameter, parameter). The constrained value is then normalized to [0,1) and linearly scaled to fit the desired integer range [min, max].

The parameter controls the width of the distribution - larger parameter values allow more variation from the center of the range, while smaller values concentrate the results closer to the middle. The minimum allowed parameter value is 2.0 (defined by ), which provides reasonable performance characteristics for the rejection sampling loop.

## Parameters / Member Variables
- : Pointer to the PRNG state structure providing the source of randomness
- : Lower bound of the output range (inclusive)
- : Upper bound of the output range (inclusive)  
- : Gaussian distribution parameter (must be >= 2.0) controlling the distribution width

## Dependencies
- Functions called/Symbols referenced:
  - pg_prng_state (PostgreSQL PRNG state type)
  - MIN_GAUSSIAN_PARAM (minimum parameter constant, value 2.0)
  - [pg_prng_double_normal](../p/pg_prng_double_normal.md) (PostgreSQL normal distribution generator)
  - Assert (PostgreSQL assertion macro)
- Called from (representative examples):
  - evalStandardFunc (at src/bin/pgbench/pgbench.c:2699)

## Notes and Other Information
- Function is declared static, limiting its scope to the pgbench.c file
- The parameter must be >= 2.0 and is validated with an assertion  
- Uses rejection sampling to constrain normal random values to [-parameter, parameter) range
- The rejection loop has low probability of iteration: ~8.6% for parameter=2.0, ~0.43% for parameter=5.0
- Mathematical analysis shows the looping probability decreases exponentially with larger parameter values
- The constrained normal value is linearly normalized to [0,1) then scaled to [min, max] integer range
- Part of pgbench's statistical distribution capabilities for realistic workload modeling
- Gaussian distribution is particularly useful for simulating phenomena that cluster around a central value with symmetric variations
- The implementation ensures that extreme outliers are rejected while maintaining the Gaussian distribution properties within the specified bounds