# pg_prng_seed_check

## Location
src/common/pg_prng.c: 114 - 133

## Overview
Validates a PRNG seed value by ensuring the state contains non-zero values to prevent degenerate random number generation.

## Definition
```c
bool pg_prng_seed_check(pg_prng_state *state)
```

## Detailed Description
This function performs validation on a pseudo-random number generator state after seeding. The primary purpose is to handle the edge case where the seeding mechanism produces all-zero values, which would result in a degenerate PRNG that only produces zeros. When this situation is detected, the function replaces the zero values with Knuth's LCG (Linear Congruential Generator) parameters to ensure proper random number generation.

The function checks if both state components (`s0` and `s1`) are zero, and if so, initializes them with predetermined non-zero values. This validation step is crucial for maintaining the quality of random number generation across the PostgreSQL system.

## Parameters / Member Variables
- `state`: Pointer to a `pg_prng_state` structure containing the PRNG state to validate. The state consists of two 64-bit unsigned integers (`s0` and `s1`) that form the internal state of the random number generator.

## Dependencies
- Functions called/Symbols referenced:
  - pg_prng_state (struct type)
  - UINT64CONST (macro)
  - unlikely (macro)
- Called from (representative examples):
  - pg_prng_seed
  - pg_prng_strong_seed (macro)

## Notes and Other Information
- The function always returns `true` as a convenience for the `pg_prng_strong_seed` macro
- Uses Knuth's LCG parameters (0x5851F42D4C957F2D and 0x14057B7EF767814F) as fallback values
- The `unlikely` macro is used to optimize the branch prediction for the rare case of all-zero state
- This validation is essential for cryptographic and statistical quality of the PRNG output
- Located in `src/common/pg_prng.c` at lines 114-133