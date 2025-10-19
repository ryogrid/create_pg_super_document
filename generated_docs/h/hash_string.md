# hash_string

## Location
[src/include/common/hashfn_unstable.h:391-407](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/hashfn_unstable.h#L391-L407)

## Overview
A convenience function for hashing NUL-terminated C strings, providing a simple interface to PostgreSQL's fasthash algorithm specifically optimized for string data.

## Definition

```c
static inline uint32
hash_string(const char *s)
```
## Detailed Description
The hash_string function provides a streamlined interface for hashing NUL-terminated strings using PostgreSQL's incremental fasthash framework. It automatically handles string length determination and applies appropriate length-based mixing in the final hash computation.

The function uses the fasthash_accum_cstring function which is specifically designed for C strings, automatically detecting the string length while accumulating the hash. The detected length is then used in the final mixing step via fasthash_final32, ensuring that strings of different lengths produce well-distributed hash values even when they share common prefixes.

## Parameters / Member Variables
- `*s`: Pointer to a NUL-terminated C string to be hashed (const char*)
## Dependencies
- Functions called/Symbols referenced:
  - [fasthash_state](../f/fasthash_state.md) (hash state structure)
  - [fasthash_init](../f/fasthash_init.md) (hash state initialization)
  - [fasthash_accum_cstring](../f/fasthash_accum_cstring.md) (C string accumulation with automatic length detection)
  - [fasthash_final32](../f/fasthash_final32.md) (finalization returning 32-bit hash with length mixing)
- Called from (representative examples):
  - SH_HASH_KEY (used in multiple PostgreSQL utilities as hash function for simple hash tables)
  - load_manifest.c, pg_dumpall.c, filemap.c, pg_verifybackup.c (various PostgreSQL utilities)

## Notes and Other Information
- Declared as static inline for optimal performance in header file src/include/common/hashfn_unstable.h
- Specifically designed for NUL-terminated strings, eliminating the need to pre-compute string length
- Returns a 32-bit hash value suitable for most hash table applications
- The automatic length detection and length-based final mixing ensure good hash distribution
- Widely used throughout PostgreSQL utilities for hashing string keys in simple hash tables
- Part of PostgreSQL's unstable hash function family, meaning hash values may change between versions
- More convenient than fasthash32 for string hashing as it doesn't require explicit length calculation
- The length mixing in fasthash_final32 helps avoid hash collisions between strings with common prefixes

## Simplified Source

```c
static inline uint32
hash_string(const char *s)
{
    fasthash_state hs;
    size_t s_len;

    // Initialize hash state
    fasthash_init(&hs, 0);

    // Hash the string and capture its length
    s_len = fasthash_accum_cstring(&hs, s);

    // Finalize with length mixing for better distribution
    return fasthash_final32(&hs, s_len);
}
```