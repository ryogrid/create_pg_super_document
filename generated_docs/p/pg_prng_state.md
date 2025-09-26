# pg_prng_state

## Location
src/include/common/pg_prng.h: 19 - 23

## Overview
`pg_prng_state` is a structure that represents the internal state vector for PostgreSQL's pseudo-random number generator (PRNG), implementing the xoroshiro128** algorithm for high-quality random number generation.

## Definition
```c
typedef struct pg_prng_state
{
    uint64  s0,
            s1;
} pg_prng_state;
```

## Detailed Description
The `pg_prng_state` structure serves as the core state container for PostgreSQL's pseudo-random number generation system. It implements the xoroshiro128** algorithm, which is a fast, high-quality pseudo-random number generator developed by David Blackman and Sebastiano Vigna.

The structure is designed as an opaque type from the caller's perspective, meaning users should not directly access or modify the internal state members. However, the definition is exposed in the header file to allow embedding within other structures for performance reasons and to avoid dynamic memory allocation.

The PRNG system uses a 128-bit state (two 64-bit values) and produces 64-bit output values. The algorithm ensures uniform distribution across the full uint64 range and has excellent statistical properties with a very long period (2^128 - 1). The state must never be all-zeroes, as this represents a fixed point that would cause the generator to produce only zero values.

The state is typically initialized using functions like `pg_prng_seed()` or `pg_prng_strong_seed()`, and random values are generated through functions like `pg_prng_uint64()`, `pg_prng_double()`, etc.

## Parameters / Member Variables
- `s0`: First 64-bit component of the 128-bit PRNG state vector, used in the xoroshiro128** algorithm
- `s1`: Second 64-bit component of the 128-bit PRNG state vector, used in the xoroshiro128** algorithm

## Dependencies
- Functions that use this structure:
  - `pg_prng_seed` - Initialize the state from a 64-bit seed
  - `pg_prng_fseed` - Initialize the state from floating-point seed
  - `pg_prng_strong_seed` - Initialize with cryptographically strong seed
  - `pg_prng_seed_check` - Verify state is not all-zeros
  - `pg_prng_uint64` - Generate 64-bit unsigned random number
  - `pg_prng_uint64_range` - Generate random number in specific range
  - `pg_prng_int64` - Generate 64-bit signed random number
  - `pg_prng_int32` - Generate 32-bit signed random number
  - `pg_prng_uint32` - Generate 32-bit unsigned random number
  - `pg_prng_double` - Generate double-precision floating-point random number
  - `pg_prng_double_normal` - Generate normally distributed random number
  - `pg_prng_bool` - Generate random boolean value
  - `xoroshiro128ss` - Core algorithm implementation (internal)

- Used extensively by:
  - `pgbench` for workload generation and randomization
  - Numeric/sampling utilities for statistical operations
  - Various PostgreSQL subsystems requiring random number generation
  - Connection state management in libpq
  - Test modules for randomized testing

## Notes and Other Information
- The structure implements the xoroshiro128** algorithm, which provides excellent performance and statistical quality
- State must never be all-zeroes; initialization functions ensure this constraint
- The structure is designed to be embedded in other structures to avoid dynamic allocation overhead
- The PRNG has a period of 2^128 - 1, making it suitable for long-running applications
- All state transitions are performed through dedicated functions; direct state manipulation should be avoided
- The algorithm is not cryptographically secure; use `pg_prng_strong_seed()` when cryptographic properties are required for initialization
- Thread safety depends on the calling context; each thread should typically maintain its own state instance