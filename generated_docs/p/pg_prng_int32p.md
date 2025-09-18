# pg_prng_int32p

## Location
[src/common/pg_prng.c:254-267](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/pg_prng.c#L254-L267)

## Overview
Generates a random 32-bit signed integer uniformly distributed over the positive range [0, PG_INT32_MAX].

## Definition


## Detailed Description
This function selects a random int32 uniformly from the range [0, PG_INT32_MAX], providing only non-negative values. The key difference from pg_prng_int32 is the use of a right shift by 33 bits (v >> 33) instead of 32 bits, which effectively removes the sign bit and ensures the result is always non-negative. This gives a uniform distribution over 31 bits of positive values.

## Parameters / Member Variables
- : Pointer to the pseudo-random number generator state structure

## Dependencies
- Functions called/Symbols referenced:
  - xoroshiro128ss (the core PRNG algorithm)
  - pg_prng_state (state structure type)
- Called from (representative examples):
  - create_and_test_bloom (in test_bloomfilter module)

## Notes and Other Information
- Uses 33-bit right shift (v >> 33) to ensure non-negative results
- Provides uniform distribution over [0, PG_INT32_MAX] range
- Useful when only positive integers are needed
- The 'p' suffix indicates 'positive' values only
- Part of PostgreSQL's unified PRNG interface for consistent random number generation
- Less commonly used than other PRNG functions, primarily in specialized contexts