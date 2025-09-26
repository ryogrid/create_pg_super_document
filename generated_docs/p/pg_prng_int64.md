# pg_prng_int64

## Location
[src/common/pg_prng.c:173-181](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/pg_prng.c#L173-L181)

## Overview
Generates a random 64-bit signed integer uniformly distributed across the full range [PG_INT64_MIN, PG_INT64_MAX].

## Definition
```c
int64 pg_prng_int64(pg_prng_state *state)
```

## Detailed Description
This function provides a simple interface for generating signed 64-bit random integers by casting the output of the underlying unsigned PRNG algorithm. It leverages the full output range of `xoroshiro128ss` and reinterprets the bit pattern as a signed integer, effectively mapping the unsigned range [0, 2^64-1] to the signed range [−2^63, 2^63−1].

The function maintains uniform distribution across the entire signed integer range by directly casting the unsigned result. This approach is mathematically sound because the bit patterns are uniformly distributed, and the two's complement representation preserves this uniformity when interpreted as signed values.

## Parameters / Member Variables
- `state`: Pointer to a `pg_prng_state` structure that maintains the internal state of the pseudo-random number generator. The state is modified during the generation process.

## Dependencies
- Functions called/Symbols referenced:
  - [pg_prng_state](pg_prng_state.md) (struct type)
  - [xoroshiro128ss](../x/xoroshiro128ss.md) (internal PRNG algorithm implementation)
- Called from (representative examples):
  - pg_prng_strong_seed (macro)

## Notes and Other Information
- Returns values uniformly distributed across the complete int64 range [−2^63, 2^63−1]
- Uses direct casting from unsigned to signed, preserving uniform distribution
- Simpler than the unsigned version as it doesn't require additional range handling
- The underlying xoroshiro128** algorithm ensures high-quality randomness
- Less commonly used compared to range-specific functions, but serves as a foundation
- Located in `src/common/pg_prng.c` at lines 173-181