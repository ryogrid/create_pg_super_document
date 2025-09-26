# pg_prng_seed

## Location
[src/common/pg_prng.c:89-101](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/pg_prng.c#L89-L101)

## Overview
The `pg_prng_seed` function initializes a PostgreSQL pseudo-random number generator state from a 64-bit integer seed, ensuring the resulting state is valid for the xoroshiro128** algorithm.

## Definition
```c
void pg_prng_seed(pg_prng_state *state, uint64 seed)
```

## Detailed Description
The `pg_prng_seed` function serves as the primary initialization interface for PostgreSQL's pseudo-random number generator. It takes a single 64-bit seed value and uses the SplitMix64 algorithm to generate the 128-bit state required by the xoroshiro128** generator.

The function performs the following operations:
1. Uses the `splitmix64` function twice with the provided seed to generate two different 64-bit values
2. Assigns the first generated value to `state->s0`
3. Assigns the second generated value to `state->s1`
4. Calls `pg_prng_seed_check` to ensure the resulting state is not all-zeroes (which would be a degenerate case for xoroshiro128**)

This approach ensures that even if consecutive or similar seed values are used, the resulting states will be well-distributed and statistically independent. The SplitMix64 algorithm is particularly good at producing high-quality output from potentially poor input seeds, making it ideal for this initialization purpose.

## Parameters / Member Variables
- `state`: Pointer to a pg_prng_state structure that will be initialized with the new seed
- `seed`: 64-bit unsigned integer used as the seed value for initialization

## Dependencies
- Functions called/Symbols referenced:
  - [splitmix64](../s/splitmix64.md) (called twice to generate s0 and s1 values)
  - [pg_prng_seed_check](pg_prng_seed_check.md) (called to validate the resulting state)
  - [pg_prng_state](pg_prng_state.md) (state structure type)
- Called from (representative examples):
  - [InitProcessGlobals](../I/InitProcessGlobals.md) (process initialization)
  - [initialize_prng](../i/initialize_prng.md) (pseudorandom function initialization)
  - [sampler_random_init_state](../s/sampler_random_init_state.md) (sampling utilities)
  - [choose_dsm_implementation](../c/choose_dsm_implementation.md) (initdb)
  - [setup_publisher](../s/setup_publisher.md) (pg_createsubscriber)
  - [initRandomState](../i/initRandomState.md) (pgbench)
  - [libpq_prng_init](../l/libpq_prng_init.md) (libpq connection handling)
  - Various test modules and utilities

## Notes and Other Information
- This is a public function (no static modifier), making it available to other parts of PostgreSQL
- The function ensures thread-safety by operating only on the provided state parameter
- Using the same seed will always produce the same sequence of random numbers, which is important for reproducible testing and debugging
- The seed value is passed by value and modified during the splitmix64 calls, but the original caller's seed value remains unchanged
- This function is widely used throughout PostgreSQL for initializing random number generators in various subsystems including the postmaster, pgbench, libpq, and testing modules
- The initialization process specifically avoids the all-zero state which would cause the xoroshiro128** algorithm to always generate zero
- Located in src/common/pg_prng.c, making it available to both frontend and backend code