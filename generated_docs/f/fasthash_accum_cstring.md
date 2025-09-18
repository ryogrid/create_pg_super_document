# fasthash_accum_cstring

## Location
[src/include/common/hashfn_unstable.h:289-324](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/hashfn_unstable.h#L289-L324)

## Overview
An optimized inline function that accumulates a C string into a fast hash state, automatically choosing between aligned and unaligned processing paths for optimal performance on 64-bit platforms.

## Definition
```c
static inline size_t
fasthash_accum_cstring(fasthash_state *hs, const char *str)
```

## Detailed Description
This function is part of PostgreSQL's fast hashing implementation for C strings. It provides an optimized path selection mechanism that:

1. **On 64-bit platforms (SIZEOF_VOID_P >= 8)**: Automatically detects if the input string pointer is aligned to uint64 boundaries
2. **Aligned path**: Uses `fasthash_accum_cstring_aligned()` for faster 8-byte-at-a-time processing when the string is properly aligned
3. **Unaligned path**: Falls back to `fasthash_accum_cstring_unaligned()` for unaligned pointers or 32-bit platforms
4. **32-bit platforms**: Always uses the unaligned version since word-at-a-time optimization isn't worthwhile

The function includes debugging assertions when `USE_ASSERT_CHECKING` is enabled to verify that both aligned and unaligned paths produce identical results, ensuring correctness of the optimization.

## Parameters / Member Variables
- `hs`: Pointer to the fasthash_state structure that maintains the current hash computation state
- `str`: Pointer to the null-terminated C string to be hashed

## Dependencies
- Functions called/Symbols referenced:
  - [fasthash_state](fasthash_state.md) (hash state structure)
  - `fasthash_accum_cstring_aligned` (optimized aligned processing)
  - [fasthash_accum_cstring_unaligned](fasthash_accum_cstring_unaligned.md) (general unaligned processing)
  - `PointerIsAligned` (alignment checking macro)
  - `memcpy` (for debug state copying)
- Called from (representative examples):
  - [spcachekey_hash](../s/spcachekey_hash.md) (in src/backend/catalog/namespace.c:268)
  - [hash_string](../h/hash_string.md) (in src/include/common/hashfn_unstable.h:402)

## Notes and Other Information
- Returns the length of the processed string (excluding null terminator)
- Uses compile-time and runtime optimizations to maximize performance
- The aligned version uses the `pg_attribute_no_sanitize_address()` attribute to bypass AddressSanitizer checks for direct memory access optimization
- Debug builds include verification that aligned and unaligned paths produce identical results
- Part of PostgreSQL's internal hashing infrastructure for hash tables and indexes