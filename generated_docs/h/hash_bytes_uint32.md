# hash_bytes_uint32

## Location
[src/common/hashfn.c:610-630](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/hashfn.c#L610-L630)

## Overview
The `hash_bytes_uint32` function provides optimized hashing of single 32-bit values without requiring memory storage, offering equivalent results to `hash_bytes` but with better performance.

## Definition
```c
uint32 hash_bytes_uint32(uint32 k)
```

## Detailed Description
The `hash_bytes_uint32` function is a specialized, performance-optimized variant of `hash_bytes` designed specifically for hashing single 32-bit values. Rather than requiring the caller to store the value in memory and pass a pointer (as would be needed with `hash_bytes(&k, sizeof(uint32))`), this function accepts the value directly, eliminating memory operations and improving performance.

The function uses the same initialization constants and final mixing as the core hash algorithm but skips the complex data processing loops since it only handles a single 32-bit input. It initializes the three-variable state (a, b, c) with the same magic constants plus the size of a uint32, adds the input value to variable `a`, and applies the final mixing to produce the hash result.

This optimization is particularly valuable for hash tables that use 32-bit keys, such as OID-based lookups, where the direct value hashing avoids the overhead of memory pointer operations.

## Parameters / Member Variables
- `k`: The 32-bit value to be hashed

## Dependencies
- Functions called/Symbols referenced:
  - final (final hash value computation)
- Called from (representative examples):
  - [hashagg_spill_tuple](hashagg_spill_tuple.md) (src/backend/executor/nodeAgg.c:2968)
  - [json_unique_hash](../j/json_unique_hash.md) (src/backend/utils/adt/json.c:894)
  - [uint32_hash](../u/uint32_hash.md) (src/common/hashfn.c:691)
  - ROTATE_HIGH_AND_LOW_32BITS (src/include/common/hashfn.h:26)
  - [hash_uint32](hash_uint32.md) (src/include/common/hashfn.h:45)

## Notes and Other Information
- Functionally equivalent to `hash_bytes(&k, sizeof(uint32))` but significantly faster
- Eliminates the need for memory storage and pointer dereferencing of the input value
- Uses the same cryptographic-quality mixing and constants as the full hash_bytes function
- Particularly useful for hash tables with 32-bit integer keys (OIDs, counters, etc.)
- Part of PostgreSQL's optimized hash function family for common data types
- Maintains the same hash quality and avalanche properties despite the simplified implementation

## Simplified Source

```c
// Simplified version of hash_bytes_uint32
uint32 hash_bytes_uint32(uint32 k) {
    uint32 a, b, c;

    // Initialize hash state with magic constant and size info
    a = b = c = 0x9e3779b9 + sizeof(uint32) + 3923095;

    // Add the input value to the hash state
    a += k;

    // Apply final mixing to produce the hash result
    final(a, b, c);

    return c;
}
```

Key simplifications made:
- Preserved the essential hash initialization and mixing algorithm
- Added descriptive comments explaining each core step
- Maintained the exact logic flow without any complex preprocessing
- Kept the critical `final()` mixing function call that ensures hash quality