# getrand

## Location
[src/bin/pgbench/pgbench.c:1102-1112](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L1102-L1112)

## Overview
Generates a uniformly distributed random integer within a specified inclusive range using PostgreSQL's pseudo-random number generator.

## Definition

```c
static int64
getrand(pg_prng_state *state, int64 min, int64 max)
```
## Detailed Description
The  function provides uniform random number generation within a specified inclusive range [min, max] for pgbench operations. It leverages PostgreSQL's  function to generate random values and performs the necessary arithmetic to map them to the desired range. The function is designed to handle 64-bit integer ranges while ensuring uniform distribution across the entire specified interval.

The implementation uses a simple linear transformation: it generates a random value in the range [0, max-min] and then adds the minimum value to shift the result to the desired range. This approach maintains the uniform distribution property of the underlying PRNG.

An important limitation is that the difference between max and min must not overflow int64, though this constraint is not actively checked by the function.

## Parameters / Member Variables
- `*state`: Pointer to the PRNG state structure that provides the source of randomness
- `min`: Lower bound of the range (inclusive)
- `max`: Upper bound of the range (inclusive)
## Dependencies
- Functions called/Symbols referenced:
  - [pg_prng_state](../p/pg_prng_state.md) (PostgreSQL PRNG state type)
  - [pg_prng_uint64_range](../p/pg_prng_uint64_range.md) (PostgreSQL PRNG range function)
- Called from (representative examples):
  - [evalStandardFunc](../e/evalStandardFunc.md) (at src/bin/pgbench/pgbench.c:2678)
  - [chooseScript](../c/chooseScript.md) (at src/bin/pgbench/pgbench.c:3055)

## Notes and Other Information
- Function is declared static, limiting its scope to the pgbench.c file
- Returns int64 values within the inclusive range [min, max]
- The difference (max - min) must not overflow int64, but this is not validated
- Provides uniform distribution across the entire specified range
- Critical for pgbench's random data generation and script selection
- Used extensively in benchmark operations that require random value selection
- The underlying  ensures high-quality randomness suitable for benchmarking
- Both min and max bounds are inclusive in the generated range

## Simplified Source

```c
static int64 getrand(pg_prng_state *state, int64 min, int64 max) {
    // Generate uniform random value in range [min, max] inclusive
    // Uses linear transformation: random_in_[0, max-min] + min
    return min + (int64) pg_prng_uint64_range(state, 0, max - min);
}
```

**Key Points:**
- Generates uniformly distributed random integers in inclusive range [min, max]
- Uses linear transformation to map [0, max-min] to [min, max]
- Limitation: (max - min) must not overflow int64 (not validated)
- Essential for pgbench's random data generation and script selection