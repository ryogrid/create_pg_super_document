# hash_bytes_extended

## Location
src/common/hashfn.c: 372 - 609

## Overview
The `hash_bytes_extended` function is an enhanced version of `hash_bytes` that produces 64-bit hash values and supports optional seeding for cryptographic applications and enhanced security.

## Definition
```c
uint64 hash_bytes_extended(const unsigned char *k, int keylen, uint64 seed)
```

## Detailed Description
The `hash_bytes_extended` function extends the core `hash_bytes` algorithm to provide 64-bit output and seed support. It maintains the same excellent avalanche properties and performance characteristics as `hash_bytes` while offering additional security through seeding and expanded output space.

The function uses the same three-variable internal state (a, b, c) but incorporates the seed by treating it as part of the data being hashed. When a non-zero seed is provided, it's split into its upper and lower 32-bit components and mixed into the internal state as if it were a 12-byte data chunk padded with four zero bytes.

Like `hash_bytes`, it processes data efficiently in 12-byte chunks with optimized paths for aligned and non-aligned memory access, handling endianness differences through conditional compilation. The 64-bit result is constructed by combining the final values of variables b (upper 32 bits) and c (lower 32 bits).

## Parameters / Member Variables
- `k`: The unaligned variable-length array of bytes to be hashed (the key)
- `keylen`: The length of the key in bytes  
- `seed`: A 64-bit seed value (0 means no seed is used)

## Dependencies
- Functions called/Symbols referenced:
  - mix (internal hash mixing function)
  - UINT32_ALIGN_MASK (for memory alignment checking)
  - final (final hash value computation)
- Called from (representative examples):
  - ROTATE_HIGH_AND_LOW_32BITS (src/include/common/hashfn.h:24)
  - hash_any_extended (src/include/common/hashfn.h:39)

## Notes and Other Information
- Provides 64-bit hash values by combining both b and c final values, compared to hash_bytes which only returns c
- Seed functionality allows for salted hashing, useful for security applications and avoiding hash collision attacks
- The seed is treated as an additional 12-byte data chunk (with implicit zero padding) during the mixing process
- Maintains the same performance characteristics and safety guarantees as hash_bytes
- Essential for applications requiring larger hash spaces or cryptographic seeding capabilities
- Used as the foundation for other extended hash functions in PostgreSQL's hash function family