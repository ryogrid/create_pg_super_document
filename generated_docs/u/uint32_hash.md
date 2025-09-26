# uint32_hash

## Location
[src/common/hashfn.c:688-692](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/hashfn.c#L688-L692)

## Overview
The `uint32_hash` function is an optimized hash function specifically designed for hashing 32-bit unsigned or signed integer keys, providing better performance than the generic `tag_hash` function for this common use case.

## Definition
```c
uint32 uint32_hash(const void *key, Size keysize)
```

## Detailed Description
`uint32_hash` is a specialized hash function optimized for 32-bit integer values (both uint32 and int32). Unlike the more general `tag_hash` function, this implementation leverages the optimized `hash_bytes_uint32` function which is specifically designed for single 32-bit values, avoiding the overhead of generic byte-array hashing.

The function includes an assertion to ensure that only 32-bit values are passed (keysize must equal sizeof(uint32)), providing type safety and preventing misuse. This makes it an ideal choice for hash tables that use 32-bit integers as keys, such as OID-based lookups, process IDs, or other integer identifiers commonly used throughout PostgreSQL.

## Parameters / Member Variables
- `key`: A pointer to the 32-bit integer value to be hashed
- `keysize`: The size in bytes of the key (must be sizeof(uint32) = 4 bytes)

## Dependencies
- Functions called/Symbols referenced:
  - [hash_bytes_uint32](../h/hash_bytes_uint32.md)
- Called from (representative examples):
  - [hash_create](../h/hash_create.md)
  - [hash_uint32_extended](../h/hash_uint32_extended.md)
  - oid_hash

## Notes and Other Information
- This function is optimized specifically for 32-bit integer hashing and provides better performance than `tag_hash` for this use case
- Includes runtime assertion to ensure correct usage (keysize must equal sizeof(uint32))
- Leverages the specialized `hash_bytes_uint32` function for optimal performance
- Commonly used for OID-based hash tables and other integer key scenarios in PostgreSQL
- The comment in the source explicitly notes that while `tag_hash` can handle this case, `uint32_hash` is faster