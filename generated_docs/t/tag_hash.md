# tag_hash

## Location
[src/common/hashfn.c:677-687](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/hashfn.c#L677-L687)

## Overview
The `tag_hash` function is a general-purpose hash function wrapper designed for hashing fixed-size tag values, providing a convenient interface to hash arbitrary data structures used as hash table keys.

## Definition
```c
uint32 tag_hash(const void *key, Size keysize)
```

## Detailed Description
`tag_hash` serves as a standardized hash function interface for PostgreSQL's hash table implementations when dealing with fixed-size data structures (tags). It acts as a thin wrapper around the core `hash_bytes` function, providing type safety and a consistent API for hashing operations on structured data.

The function is commonly used in hash table creation where fixed-size keys need to be hashed, such as cache lookups, index structures, or any scenario where structured data needs to be used as hash keys. It ensures consistent hashing behavior across different parts of the PostgreSQL codebase.

## Parameters / Member Variables
- `key`: A pointer to the data to be hashed (treated as opaque bytes)
- `keysize`: The size in bytes of the data structure being hashed

## Dependencies
- Functions called/Symbols referenced:
  - [hash_bytes](../h/hash_bytes.md)
- Called from (representative examples):
  - [dshash_memhash](../d/dshash_memhash.md)
  - [hash_create](../h/hash_create.md)
  - [hash_uint32_extended](../h/hash_uint32_extended.md)

## Notes and Other Information
- This function provides a consistent interface for hashing fixed-size structures across PostgreSQL
- It leverages the optimized `hash_bytes` implementation for the actual hash computation
- The function is designed to work with any fixed-size data structure, making it highly reusable
- Commonly used in hash table operations where structured keys need deterministic hash values