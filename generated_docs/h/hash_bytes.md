# hash_bytes

## Location
src/common/hashfn.c: 146 - 371

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
  - tag_hash (src/common/hashfn.c:679)
  - [hash_any](hash_any.md) (src/include/common/hashfn.h:33)

## Notes and Other Information
- The function provides excellent hash distribution with avalanche properties for cryptographic-quality hashing
- Optimized for both aligned and non-aligned memory access patterns
- Uses endian-aware byte ordering for consistent results across different architectures  
- Critical constraint: must never call `elog(ERROR)` to maintain system stability
- The algorithm could easily be extended to return 64-bit values using both b and c final values
- Designed for optimal performance with power-of-2 hash table sizes (no expensive modulo operations needed)