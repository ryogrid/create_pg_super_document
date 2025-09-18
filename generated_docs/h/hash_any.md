# hash_any

## Location
[src/include/common/hashfn.h:31-36](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/hashfn.h#L31-L36)

## Overview
The `hash_any` function serves as a standard interface for hashing arbitrary byte sequences, converting the result into PostgreSQL's Datum format for use in hash-based data structures and operations.

## Definition
```c
static inline Datum hash_any(const unsigned char *k, int keylen)
```

## Detailed Description
`hash_any` is a lightweight wrapper around the core `hash_bytes` function that provides a consistent interface for PostgreSQL's hash table infrastructure. It takes arbitrary binary data and produces a 32-bit hash value wrapped in a Datum type, making it suitable for use with PostgreSQL's internal type system. The function is implemented as a static inline function for optimal performance, as it's frequently called throughout the system for hashing various data types.

The function leverages the robust `hash_bytes` algorithm internally while handling the type conversion necessary for PostgreSQL's hash table APIs. This design allows for consistent hashing behavior across different data types while maintaining the performance characteristics of the underlying hash algorithm.

## Parameters / Member Variables
- `k`: Pointer to the byte array to be hashed
- `keylen`: Length of the byte array in bytes

## Dependencies
- Functions called/Symbols referenced:
  - [hash_bytes](hash_bytes.md)
  - [UInt32GetDatum](../U/UInt32GetDatum.md)
- Called from (representative examples):
  - [hashfloat4](hashfloat4.md)
  - [hashfloat8](hashfloat8.md)
  - [hashtext](hashtext.md)
  - [hashvarlena](hashvarlena.md)
  - [hash_numeric](hash_numeric.md)
  - [uuid_hash](../u/uuid_hash.md)
  - [hashinet](hashinet.md)
  - [hashmacaddr](hashmacaddr.md)

## Notes and Other Information
This function is widely used throughout PostgreSQL's hash infrastructure, serving as the standard entry point for hashing operations on various data types including floating-point numbers, text, network addresses, UUIDs, and numeric types. Its inline implementation ensures minimal overhead when used in performance-critical hash table operations.