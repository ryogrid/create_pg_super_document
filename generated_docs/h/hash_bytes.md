# hash_bytes

## Location
[src/common/hashfn.c:146-371](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/hashfn.c#L146-L371)

## Overview
The `hash_bytes` function is a core hash function that transforms variable-length byte arrays into 32-bit hash values, designed for high-performance hash table operations in PostgreSQL.

## Definition
```c
uint32 hash_bytes(const unsigned char *k, int keylen)
```

## Detailed Description
The `hash_bytes` function implements a sophisticated hash algorithm optimized for PostgreSQL's hash table requirements. It processes variable-length byte arrays and produces 32-bit hash values with excellent avalanche properties - every bit of the input affects every bit of the output, and small input changes (1-bit or 2-bit deltas) produce large output changes.

The function uses an internal state consisting of three 32-bit variables (a, b, c) initialized with magic constants and the key length. It processes data in 12-byte chunks when possible for efficiency, with separate optimized code paths for aligned and non-aligned memory access. The algorithm handles endianness differences with conditional compilation directives.

The implementation guarantees that it will never throw `elog(ERROR)`, making it safe for use in critical system components like the ResourceOwner code. The function is designed to run in approximately 6*len+35 instructions and works optimally with power-of-2 hash table sizes.

## Parameters / Member Variables
- `k`: The unaligned variable-length array of bytes to be hashed (the key)
- `keylen`: The length of the key in bytes

## Dependencies
- Functions called/Symbols referenced:
  - UINT32_ALIGN_MASK (for memory alignment checking)
  - mix (internal hash mixing function)
  - final (final hash value computation)
- Called from (representative examples):
  - [missing_hash](../m/missing_hash.md) (src/backend/access/common/heaptuple.c:104)
  - [hash_string_pointer](hash_string_pointer.md) (src/backend/backup/basebackup_incremental.c:925)
  - [datum_image_hash](../d/datum_image_hash.md) (src/backend/utils/adt/datum.c:344,346,355,368)
  - [json_unique_hash](../j/json_unique_hash.md) (src/backend/utils/adt/json.c:896)
  - [string_hash](../s/string_hash.md) (src/common/hashfn.c:670)
  - [tag_hash](../t/tag_hash.md) (src/common/hashfn.c:679)
  - [hash_any](hash_any.md) (src/include/common/hashfn.h:33)

## Notes and Other Information
- The function provides excellent hash distribution with avalanche properties for cryptographic-quality hashing
- Optimized for both aligned and non-aligned memory access patterns
- Uses endian-aware byte ordering for consistent results across different architectures
- Critical constraint: must never call `elog(ERROR)` to maintain system stability
- The algorithm could easily be extended to return 64-bit values using both b and c final values
- Designed for optimal performance with power-of-2 hash table sizes (no expensive modulo operations needed)

## Simplified Source

```c
// Simplified version of hash_bytes
uint32 hash_bytes(const unsigned char *k, int keylen) {
    uint32 a, b, c, len;

    // Initialize internal state with magic constants and key length
    len = keylen;
    a = b = c = 0x9e3779b9 + len + 3923095;

    // Check if source pointer is word-aligned for optimization
    if (((uintptr_t) k & UINT32_ALIGN_MASK) == 0) {
        // Aligned access path: process in 12-byte chunks efficiently
        const uint32 *ka = (const uint32 *) k;

        // Process most of the key in 12-byte chunks
        while (len >= 12) {
            a += ka[0];
            b += ka[1];
            c += ka[2];
            mix(a, b, c);  // Hash mixing function
            ka += 3;
            len -= 12;
        }

        // Handle remaining bytes (0-11) with endian-aware byte packing
        k = (const unsigned char *) ka;
        // Simplified: pack remaining bytes into a, b, c based on length
        // (Original has detailed switch statements for each byte position)
        pack_remaining_bytes_aligned(k, len, &a, &b, &c);
    } else {
        // Non-aligned access path: manual byte assembly
        while (len >= 12) {
            // Manually assemble 32-bit values from bytes with endian handling
            a += assemble_word_from_bytes(k);      // k[0-3]
            b += assemble_word_from_bytes(k + 4);  // k[4-7]
            c += assemble_word_from_bytes(k + 8);  // k[8-11]
            mix(a, b, c);
            k += 12;
            len -= 12;
        }

        // Handle remaining bytes with manual byte packing
        pack_remaining_bytes_unaligned(k, len, &a, &b, &c);
    }

    // Final hash computation and mixing
    final(a, b, c);

    return c;  // Return final hash value
}
```

Key simplifications made:
- Abstracted detailed endian-specific switch statements into conceptual functions
- Removed platform-specific conditional compilation blocks for clarity
- Consolidated similar byte-packing logic patterns
- Simplified variable declarations and initialization
- Maintained the core two-path algorithm (aligned vs non-aligned)
- Preserved essential hash mixing and finalization steps
- Kept the critical constraint documentation (no elog calls)