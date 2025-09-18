# getrand

## Location
src/bin/pgbench/pgbench.c: 1102 - 1112

## Overview
Generates a uniformly distributed random integer within a specified inclusive range using PostgreSQL's pseudo-random number generator.

## Definition


## Detailed Description
The  function provides uniform random number generation within a specified inclusive range [min, max] for pgbench operations. It leverages PostgreSQL's  function to generate random values and performs the necessary arithmetic to map them to the desired range. The function is designed to handle 64-bit integer ranges while ensuring uniform distribution across the entire specified interval.

The implementation uses a simple linear transformation: it generates a random value in the range [0, max-min] and then adds the minimum value to shift the result to the desired range. This approach maintains the uniform distribution property of the underlying PRNG.

An important limitation is that the difference between max and min must not overflow int64, though this constraint is not actively checked by the function.

## Parameters / Member Variables
- : Pointer to the PRNG state structure that provides the source of randomness
- : Lower bound of the range (inclusive)
- : Upper bound of the range (inclusive)

## Dependencies
- Functions called/Symbols referenced:
  - pg_prng_state (PostgreSQL PRNG state type)
  - pg_prng_uint64_range (PostgreSQL PRNG range function)
- Called from (representative examples):
  - evalStandardFunc (at src/bin/pgbench/pgbench.c:2678)
  - chooseScript (at src/bin/pgbench/pgbench.c:3055)

## Notes and Other Information
- Function is declared static, limiting its scope to the pgbench.c file
- Returns int64 values within the inclusive range [min, max]
- The difference (max - min) must not overflow int64, but this is not validated
- Provides uniform distribution across the entire specified range
- Critical for pgbench's random data generation and script selection
- Used extensively in benchmark operations that require random value selection
- The underlying  ensures high-quality randomness suitable for benchmarking
- Both min and max bounds are inclusive in the generated range