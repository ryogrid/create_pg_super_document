# hash_uint32

## Location
src/include/common/hashfn.h: 43 - 48

## Overview
The `hash_uint32` function provides optimized hashing specifically for 32-bit unsigned integer values, offering better performance than general-purpose byte array hashing for this common data type.

## Definition
```c
static inline Datum hash_uint32(uint32 k)
```

## Detailed Description
`hash_uint32` is a specialized hash function designed for efficiently hashing single 32-bit unsigned integer values. It leverages the optimized `hash_bytes_uint32` function internally, which avoids the overhead of memory operations required by general byte array hashing. This function is particularly useful for hashing integer-based keys in hash tables and other data structures where performance is critical.

The function serves as the standard interface for hashing 32-bit values in PostgreSQL's type system, automatically wrapping the result in a Datum for compatibility with the database's internal APIs. Its inline implementation ensures minimal overhead for frequently executed hash operations.

## Parameters / Member Variables
- `k`: The 32-bit unsigned integer value to be hashed

## Dependencies
- Functions called/Symbols referenced:
  - [hash_bytes_uint32](hash_bytes_uint32.md)
  - [UInt32GetDatum](../U/UInt32GetDatum.md)
- Called from (representative examples):
  - [hashchar](hashchar.md)
  - [hashint2](hashint2.md)
  - [hashint4](hashint4.md)
  - [hashint8](hashint8.md)
  - [hashoid](hashoid.md)
  - [hashenum](hashenum.md)
  - [timetz_hash](../t/timetz_hash.md)
  - [hash_range](hash_range.md)
  - [hash_multirange](hash_multirange.md)
  - [hashRowType](hashRowType.md)

## Notes and Other Information
This function is widely used throughout PostgreSQL for hashing various integer-based data types including characters, small integers, regular integers, object identifiers, and enums. It's also utilized in specialized contexts like abbreviation conversion for sorting operations and hash computations for complex types like ranges and multiranges. The optimized implementation makes it the preferred choice over general-purpose hashing when dealing with 32-bit integer values.