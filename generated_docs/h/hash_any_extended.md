# hash_any_extended

## Location
[src/include/common/hashfn.h:37-42](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/hashfn.h#L37-L42)

## Overview
The `hash_any_extended` function provides seeded hashing for arbitrary byte sequences, enabling enhanced hash distribution and collision resistance through the inclusion of a 64-bit seed value.

## Definition
```c
static inline Datum hash_any_extended(const unsigned char *k, int keylen, uint64 seed)
```

## Detailed Description
`hash_any_extended` is an enhanced version of `hash_any` that incorporates a seed parameter to produce 64-bit hash values. This function serves as the extended hashing interface for PostgreSQL's hash infrastructure, particularly useful in scenarios requiring better hash distribution or when avoiding hash collision attacks. Like its counterpart, it's implemented as a static inline function for optimal performance.

The function leverages `hash_bytes_extended` internally while handling the type conversion to PostgreSQL's Datum format. The seed parameter allows for randomization of hash values, which is essential for hash table implementations that need to resist predictable collision patterns. The 64-bit output provides a larger hash space compared to the standard 32-bit version.

## Parameters / Member Variables
- `k`: Pointer to the byte array to be hashed
- `keylen`: Length of the byte array in bytes  
- `seed`: 64-bit seed value for hash randomization

## Dependencies
- Functions called/Symbols referenced:
  - [hash_bytes_extended](hash_bytes_extended.md)
  - [UInt64GetDatum](../U/UInt64GetDatum.md)
- Called from (representative examples):
  - [hashfloat4extended](hashfloat4extended.md)
  - [hashfloat8extended](hashfloat8extended.md)
  - [hashtextextended](hashtextextended.md)
  - [hashvarlenaextended](hashvarlenaextended.md)
  - [hash_numeric_extended](hash_numeric_extended.md)
  - [uuid_hash_extended](../u/uuid_hash_extended.md)
  - [hashinetextended](hashinetextended.md)
  - [k_hashes](../k/k_hashes.md) (bloom filter)
  - [JumbleQuery](../J/JumbleQuery.md) (query fingerprinting)

## Notes and Other Information
This function is extensively used in PostgreSQL's extended hash infrastructure, particularly for the "extended" variants of hash functions across different data types. It plays a crucial role in bloom filters, query jumbling for plan caching, and anywhere enhanced hash distribution is needed. The 64-bit output and seeding capability make it suitable for applications requiring resistance to hash collision attacks.