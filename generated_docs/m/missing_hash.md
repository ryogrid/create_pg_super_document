# missing_hash

## Location
src/backend/access/common/heaptuple.c: 100 - 107

## Overview
The `missing_hash` function is a hash function specifically designed for missing attribute cache keys, computing hash values for cache lookup operations in PostgreSQL heap tuple processing.

## Definition
```c
static uint32 missing_hash(const void *key, Size keysize)
```

## Detailed Description
This function serves as a hash function callback for the missing attribute cache hash table. It extracts the value and length from a `missing_cache_key` structure and delegates to the core `hash_bytes` function to compute the actual hash. The function is designed to work with PostgreSQL hash table infrastructure, providing efficient hashing for missing attribute cache entries.

## Parameters / Member Variables
- `key`: Pointer to a `missing_cache_key` structure containing the value to be hashed
- `keysize`: Size parameter (unused in this implementation, as the actual size comes from the cache key structure)

## Dependencies
- Functions called/Symbols referenced:
  - [hash_bytes](../h/hash_bytes.md): Core hash function that performs the actual hashing
  - `missing_cache_key`: Structure type that contains the value and length to hash
- Called from (representative examples):
  - [init_missing_cache](../i/init_missing_cache.md): Used as hash function callback when initializing the missing attribute cache

## Notes and Other Information
- This is a static function, meaning it has internal linkage within heaptuple.c
- The function ignores the `keysize` parameter and instead uses the length stored in the cache key structure
- Part of PostgreSQL missing attribute cache infrastructure for optimizing tuple processing
- Returns a 32-bit hash value suitable for hash table operations