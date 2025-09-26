# pg_prng_int64p

## Location
[src/common/pg_prng.c:182-191](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/pg_prng.c#L182-L191)

## Overview
Generates a random 64-bit signed integer uniformly distributed within the positive range [0, PG_INT64_MAX].

## Definition
```c
int64 pg_prng_int64p(pg_prng_state *state)
```

## Detailed Description
This function generates non-negative signed 64-bit integers by masking off the sign bit of the underlying PRNG output. It uses a bitwise AND operation with the mask `0x7FFFFFFFFFFFFFFF` to ensure the most significant bit is always zero, effectively constraining the result to the range [0, 2^63-1].

The approach maintains uniform distribution within the positive range by taking the full 64-bit output from `xoroshiro128ss` and clearing the sign bit. This method is more efficient than using modular arithmetic or rejection sampling for this specific range, as it requires only a single bitwise operation per generated value.

The function is particularly useful for applications that require non-negative random integers but want to utilize the full precision of 64-bit arithmetic, providing twice the range of a 32-bit positive integer generator.

## Parameters / Member Variables
- `state`: Pointer to a `pg_prng_state` structure that maintains the internal state of the pseudo-random number generator. The state is modified during the generation process.

## Dependencies
- Functions called/Symbols referenced:
  - [pg_prng_state](pg_prng_state.md) (struct type)
  - [xoroshiro128ss](../x/xoroshiro128ss.md) (internal PRNG algorithm implementation)
  - UINT64CONST (constant definition macro)
- Called from (representative examples):
  - pg_prng_strong_seed (macro)

## Notes and Other Information
- Returns values uniformly distributed in the range [0, 2^63-1] (positive int64 values only)
- Uses bitwise masking with `0x7FFFFFFFFFFFFFFF` to clear the sign bit
- More efficient than modulo or rejection methods for this specific positive range
- Provides the full precision range of positive 64-bit signed integers
- The "p" suffix likely stands for "positive" to distinguish from the full-range version
- Located in `src/common/pg_prng.c` at lines 182-191