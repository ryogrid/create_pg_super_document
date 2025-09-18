# pg_prng_bool

## Location
src/common/pg_prng.c: 313 - 318

## Overview
Generates a random boolean value (true or false) with equal probability using the most significant bit of a pseudo-random number.

## Definition
```c
bool pg_prng_bool(pg_prng_state *state)
```

## Detailed Description
This function provides a simple and efficient method to generate random boolean values. It leverages the underlying xoroshiro128ss pseudo-random number generator to produce a 64-bit random value, then extracts the most significant bit (bit 63) to determine the boolean result.

The implementation is based on the principle that the most significant bit of a well-distributed random number has an equal probability of being 0 or 1, making it suitable for generating fair boolean values. By using bit shifting (v >> 63), the function extracts this single bit and converts it to a boolean type.

This approach is computationally efficient as it requires only one call to the underlying PRNG and a simple bit operation, making it suitable for high-frequency boolean random generation scenarios.

## Parameters / Member Variables
- : Pointer to the pseudo-random number generator state structure that maintains the internal state for generating random numbers

## Dependencies
- Functions called/Symbols referenced:
  - xoroshiro128ss: Core pseudo-random number generator that produces 64-bit random values
  - pg_prng_state: State structure type for the PRNG
- Called from (representative examples):
  - gistchoose: GiST index algorithm that uses random boolean decisions for tie-breaking in tree construction

## Notes and Other Information
- Provides exactly 50% probability for both true and false outcomes
- Uses the most significant bit extraction method for optimal randomness distribution
- Very efficient implementation requiring minimal computational overhead
- The underlying xoroshiro128ss generator ensures high-quality randomness properties
- Commonly used in algorithms that require random binary decisions
- Located in src/common/pg_prng.c at lines 313-318