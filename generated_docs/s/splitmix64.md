# splitmix64

## Location
[src/common/pg_prng.c:72-88](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/pg_prng.c#L72-L88)

## Overview
The `splitmix64` function implements the SplitMix64 pseudo-random number generator algorithm, used specifically to initialize the xoroshiro128** state vector from a 64-bit seed value.

## Definition
```c
static uint64 splitmix64(uint64 *state)
```

## Detailed Description
The `splitmix64` function implements the SplitMix64 algorithm developed by Guy L. Steele Jr., Doug Lea, and Christine H. Flood. This generator is specifically used in PostgreSQL to convert a single 64-bit seed into the 128-bit state required by the xoroshiro128** algorithm.

The algorithm operates in two phases:
1. **State Update**: Adds a large odd constant (0x9E3779B97f4A7C15, which is the fractional part of the golden ratio scaled to 64 bits) to the current state
2. **Value Extraction**: Applies a series of XOR-shift and multiplication operations to scramble the state value and produce high-quality output

The extraction process uses two carefully chosen 64-bit constants (0xBF58476D1CE4E5B9 and 0x94D049BB133111EB) that help ensure good statistical properties. The final output undergoes one more XOR-shift operation to further improve bit distribution.

This generator is particularly well-suited for seeding other generators because it produces well-distributed output even from poor input seeds, helping to avoid weak initial states in the main xoroshiro128** generator.

## Parameters / Member Variables
- `state`: Pointer to a 64-bit unsigned integer that maintains the generator's internal state (updated on each call)

## Dependencies
- Functions called/Symbols referenced:
  - None (uses only arithmetic and bitwise operations with constants)
- Called from (representative examples):
  - pg_prng_seed (called twice to initialize both s0 and s1 components of the xoroshiro128** state)

## Notes and Other Information
- This is a static function, accessible only within the pg_prng.c compilation unit
- The generator has a period of 2^64, suitable for its specific use case of state initialization
- The constants used are mathematically significant: 0x9E3779B97f4A7C15 represents the golden ratio, while the multiplier constants are chosen for optimal mixing properties
- Unlike the main xoroshiro128** generator, this is used only for initialization, not for ongoing random number generation
- The generator modifies the state parameter directly (it's passed by pointer), making successive calls produce different values
- SplitMix64 is known for good performance and statistical properties, making it ideal for seeding purposes
- The algorithm ensures that even consecutive or similar seed values will produce well-distributed initial states for the main generator