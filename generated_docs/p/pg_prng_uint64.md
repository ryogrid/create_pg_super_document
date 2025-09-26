# pg_prng_uint64

## Location
[src/common/pg_prng.c:134-143](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/pg_prng.c#L134-L143)

## Overview
Generates a random 64-bit unsigned integer uniformly distributed across the full range [0, PG_UINT64_MAX].

## Definition
```c
uint64 pg_prng_uint64(pg_prng_state *state)
```

## Detailed Description
This function serves as the primary interface for generating 64-bit unsigned random numbers in PostgreSQL. It wraps the underlying `xoroshiro128ss` algorithm to provide a uniform distribution across the entire range of possible 64-bit unsigned integer values. The function is the foundation for other random number generation functions that require specific ranges or data types.

The implementation uses the xoroshiro128** algorithm, which is a high-quality, fast pseudo-random number generator that passes stringent statistical tests. This makes it suitable for both general-purpose randomization and more demanding applications within the database system.

## Parameters / Member Variables
- `state`: Pointer to a `pg_prng_state` structure that maintains the internal state of the pseudo-random number generator. This state is modified during the generation process to produce the next value in the sequence.

## Dependencies
- Functions called/Symbols referenced:
  - pg_prng_state (struct type)
  - xoroshiro128ss (internal PRNG algorithm implementation)
- Called from (representative examples):
  - initRandomState (pgbench)
  - permute (pgbench)
  - pg_prng_strong_seed (macro)
  - test_random (test modules)
  - main (testint128 tool)

## Notes and Other Information
- Returns values uniformly distributed across the complete uint64 range [0, 2^64-1]
- Uses the xoroshiro128** algorithm internally, known for excellent statistical properties and performance
- Widely used throughout PostgreSQL tools and test utilities for randomization needs
- The function modifies the input state, so concurrent access requires external synchronization
- Forms the basis for other PRNG functions that provide different ranges or data types
- Located in `src/common/pg_prng.c` at lines 134-143