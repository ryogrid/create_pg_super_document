# hash_uint32_extended

## Location
src/include/common/hashfn.h: 49 - 58

## Overview
The `hash_uint32_extended` function combines the performance optimization of specialized 32-bit integer hashing with seeded hashing capabilities, producing 64-bit hash values for enhanced distribution and collision resistance.

## Definition
```c
static inline Datum hash_uint32_extended(uint32 k, uint64 seed)
```

## Detailed Description
`hash_uint32_extended` is the extended version of `hash_uint32` that incorporates a seed parameter for randomized hashing while maintaining the performance benefits of specialized 32-bit integer processing. It leverages `hash_bytes_uint32_extended` internally to provide both efficiency and enhanced hash distribution through seeding.

This function is essential for PostgreSQL's extended hash infrastructure where 64-bit hash values and collision resistance are required. The combination of optimized integer hashing with seeding makes it particularly valuable for hash tables that need to resist predictable collision patterns while maintaining high performance for integer keys.

## Parameters / Member Variables
- `k`: The 32-bit unsigned integer value to be hashed
- `seed`: 64-bit seed value for hash randomization

## Dependencies
- Functions called/Symbols referenced:
  - [hash_bytes_uint32_extended](hash_bytes_uint32_extended.md)
  - [UInt64GetDatum](../U/UInt64GetDatum.md)
  - [string_hash](../s/string_hash.md)
  - tag_hash  
  - uint32_hash
- Called from (representative examples):
  - [hashcharextended](hashcharextended.md)
  - [hashint2extended](hashint2extended.md)
  - [hashint4extended](hashint4extended.md)
  - [hashint8extended](hashint8extended.md)
  - [hashoidextended](hashoidextended.md)
  - [hashenumextended](hashenumextended.md)
  - [hash_aclitem_extended](hash_aclitem_extended.md)
  - [timetz_hash_extended](../t/timetz_hash_extended.md)
  - [hash_range_extended](hash_range_extended.md)
  - [hash_multirange_extended](hash_multirange_extended.md)

## Notes and Other Information
This function serves as the extended hashing interface for various PostgreSQL integer-based data types, providing the foundation for collision-resistant hash operations. It's particularly important in contexts where hash table security and distribution quality are critical, such as in access control lists, complex data types like ranges and multiranges, and time zone handling. The 64-bit output space and seeding capability make it suitable for production environments where hash collision attacks are a concern.