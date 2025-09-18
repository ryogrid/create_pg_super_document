# fasthash_final32

## Location
src/include/common/hashfn_unstable.h: 348 - 359

## Overview
A composite finalizer function that produces a 32-bit hash value by combining the 64-bit finalization process with intelligent reduction to preserve hash quality.

## Definition
```c
static inline uint32
fasthash_final32(fasthash_state *hs, uint64 tweak)
```

## Detailed Description
This function provides a complete 32-bit hash finalization process by composing two key operations:

1. **Finalization**: Calls `fasthash_final64()` to complete the hash computation and produce a full 64-bit hash value with the specified tweak parameter
2. **Intelligent Reduction**: Applies `fasthash_reduce32()` to reduce the 64-bit result to 32 bits using Fermat residue computation

This two-step approach ensures that the final 32-bit hash maintains high quality distribution characteristics. Rather than computing a 32-bit hash from scratch or simply truncating a 64-bit hash, it leverages the full entropy of the 64-bit computation while intelligently preserving information from both upper and lower bits during reduction.

The function serves as a convenient wrapper for applications that need 32-bit hash values but want to benefit from the full mixing quality of the 64-bit hash algorithm.

## Parameters / Member Variables
- `hs`: Pointer to the fasthash_state structure containing the accumulated hash state
- `tweak`: An optional modification parameter, typically the input length for NUL-terminated strings or zero when length is known

## Dependencies
- Functions called/Symbols referenced:
  - `fasthash_state` (hash state structure)
  - `fasthash_final64` (64-bit hash finalization)
  - `fasthash_reduce32` (64-bit to 32-bit reduction using Fermat residue)
- Called from (representative examples):
  - `spcachekey_hash` (in src/backend/catalog/namespace.c:270)
  - `hash_string` (in src/include/common/hashfn_unstable.h:404)

## Notes and Other Information
- Provides the best of both worlds: 64-bit hash quality in a 32-bit result
- More efficient than computing separate 32-bit hash algorithms while maintaining statistical quality
- The composition pattern allows for code reuse between 32-bit and 64-bit hash variants
- Used in PostgreSQL's internal hash table implementations where 32-bit hash values are sufficient
- The tweak parameter handling is identical to the 64-bit version, maintaining API consistency