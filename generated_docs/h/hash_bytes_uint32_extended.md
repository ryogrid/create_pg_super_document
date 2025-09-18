# hash_bytes_uint32_extended

## Location
src/common/hashfn.c: 631 - 659

## Overview
The `hash_bytes_uint32_extended` function combines the performance optimization of `hash_bytes_uint32` with the seeding capability and 64-bit output of `hash_bytes_extended` for efficient hashing of single 32-bit values.

## Definition
```c
uint64 hash_bytes_uint32_extended(uint32 k, uint64 seed)
```

## Detailed Description
The `hash_bytes_uint32_extended` function is a convenience function that provides the best of both worlds: the performance optimization of direct 32-bit value hashing (avoiding memory operations) combined with the enhanced security and output space of the extended hash family. This function is particularly useful for applications that need seeded hashing of integer keys.

Like `hash_bytes_uint32`, it initializes the three-variable state (a, b, c) with magic constants plus the size of a uint32. However, when a non-zero seed is provided, the seed is split into its upper and lower 32-bit components and mixed into the state before adding the input value. The function returns a 64-bit result by combining the final values of both b (upper 32 bits) and c (lower 32 bits).

This function is especially valuable for Bloom filters and other probabilistic data structures that benefit from multiple hash functions with different seeds, as seen in its usage in BRIN (Block Range Index) bloom operations.

## Parameters / Member Variables
- `k`: The 32-bit value to be hashed
- `seed`: A 64-bit seed value (0 means no seed is used)

## Dependencies
- Functions called/Symbols referenced:
  - mix (internal hash mixing function)
  - final (final hash value computation)
- Called from (representative examples):
  - [bloom_add_value](../b/bloom_add_value.md) (src/backend/access/brin/brin_bloom.c:377,378)
  - [bloom_contains_value](../b/bloom_contains_value.md) (src/backend/access/brin/brin_bloom.c:414,415)
  - ROTATE_HIGH_AND_LOW_32BITS (src/include/common/hashfn.h:27)
  - [hash_uint32_extended](hash_uint32_extended.md) (src/include/common/hashfn.h:51)

## Notes and Other Information
- Combines the performance benefits of direct uint32 hashing with extended 64-bit output and seeding
- Particularly useful for Bloom filters and probabilistic data structures requiring multiple hash functions
- Eliminates memory operations while providing cryptographic-quality hashing with salt support
- Essential for BRIN bloom index operations where multiple hash values are needed for the same key
- Part of PostgreSQL's optimized hash function family designed for high-performance indexing operations
- Returns 64-bit values by combining both final state variables (b and c) rather than just c