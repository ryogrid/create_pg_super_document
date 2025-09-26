# xoroshiro128ss

## Location
src/common/pg_prng.c: 54 - 71

## Overview
The `xoroshiro128ss` function implements the core xoroshiro128** pseudo-random number generator algorithm, producing high-quality 64-bit uniformly distributed random numbers.

## Definition
```c
static uint64 xoroshiro128ss(pg_prng_state *state)
```

## Detailed Description
The `xoroshiro128ss` function implements the xoroshiro128** (xor-rotate-shift-rotate 128-bit starstar) algorithm, which is a fast, high-quality pseudo-random number generator developed by David Blackman and Sebastiano Vigna. The algorithm maintains a 128-bit state vector (two 64-bit values s0 and s1) and produces 64-bit output values.

The algorithm performs several operations on the state vector:
1. Creates local copies of the current state values s0 and s1
2. Computes sx = s1 ^ s0 (XOR operation)
3. Generates the output value using: rotl(s0 * 5, 7) * 9 (the "**" scrambler)
4. Updates the state vector using rotation and XOR operations to ensure good statistical properties

This implementation is specifically the "starstar" variant, which uses multiplication-based output scrambling to enhance the quality of the generated random numbers. The algorithm has excellent statistical properties and is suitable for both simulation and cryptographic applications (though not cryptographically secure).

## Parameters / Member Variables
- `state`: Pointer to pg_prng_state structure containing the 128-bit state vector (s0 and s1 fields)

## Dependencies
- Functions called/Symbols referenced:
  - rotl (called 3 times for bit rotation operations)
  - pg_prng_state (state structure)
- Called from (representative examples):
  - pg_prng_uint64
  - pg_prng_uint64_range
  - pg_prng_int64
  - pg_prng_int64p
  - pg_prng_uint32
  - pg_prng_int32
  - pg_prng_int32p
  - pg_prng_double
  - pg_prng_bool

## Notes and Other Information
- The state vector must never be all-zeroes, as that creates a fixed point where the generator will always output zero
- This is a static function, accessible only within the pg_prng.c compilation unit
- The algorithm has a period of 2^128 - 1, making it suitable for demanding applications
- The xoroshiro128** algorithm is known for its speed and excellent statistical properties
- The "**" scrambler (multiplication by 5, rotation by 7, multiplication by 9) significantly improves output quality
- This generator passes all known statistical tests including BigCrush from TestU01
- The algorithm is designed to be efficiently implemented on modern 64-bit processors