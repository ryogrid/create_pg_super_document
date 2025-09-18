# fasthash_final64

## Location
src/include/common/hashfn_unstable.h: 325 - 336

## Overview
A finalizer function that completes the fast hashing process by applying a final mixing step to produce a 64-bit hash value from the accumulated hash state.

## Definition
```c
static inline uint64
fasthash_final64(fasthash_state *hs, uint64 tweak)
```

## Detailed Description
This function represents the final step in PostgreSQL's fast hash computation. It takes the current hash state that has been built up through multiple `fasthash_combine()` operations and applies a final mixing transformation using `fasthash_mix()`. 

The function serves as the termination point of the hash computation pipeline:
1. Takes the accumulated hash value from the hash state
2. Applies final mixing with an optional tweak parameter
3. Returns the final 64-bit hash value

The finalizer ensures good hash distribution by applying bit manipulation operations that spread the influence of input bits across the entire output value.

## Parameters / Member Variables
- `hs`: Pointer to the fasthash_state structure containing the accumulated hash value
- `tweak`: An optional modification parameter, typically:
  - The input length for NUL-terminated strings when length isn't known ahead of time
  - Zero when length is known or no tweaking is needed

## Dependencies
- Functions called/Symbols referenced:
  - [fasthash_state](fasthash_state.md) (hash state structure)
  - [fasthash_mix](fasthash_mix.md) (bit mixing function for final hash value refinement)
- Called from (representative examples):
  - [fasthash_final32](fasthash_final32.md) (in src/include/common/hashfn_unstable.h:350)
  - [fasthash64](fasthash64.md) (in src/include/common/hashfn_unstable.h:377)

## Notes and Other Information
- Returns a full 64-bit hash value
- The tweak parameter allows for length-dependent finalization, which is important for preventing hash collisions between strings of different lengths that share prefixes
- Used as the basis for the 32-bit finalizer (`fasthash_final32`) which reduces the 64-bit result
- Part of a two-stage design where accumulation and finalization are separated for flexibility
- The underlying `fasthash_mix` function uses bit shifts and multiplication for avalanche effect