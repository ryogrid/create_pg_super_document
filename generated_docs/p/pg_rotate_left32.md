# pg_rotate_left32

## Location
[src/include/port/pg_bitutils.h:404-411](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/pg_bitutils.h#L404-L411)

## Overview
The pg_rotate_left32 function performs a bitwise left rotation of a 32-bit unsigned integer, widely used throughout PostgreSQL's hash functions and data type hashing for improved bit distribution and hash quality.

## Definition

```c
static inline uint32
pg_rotate_left32(uint32 word, int n)
```
## Detailed Description
pg_rotate_left32 implements a circular left bit shift operation on a 32-bit unsigned integer. Unlike a regular left shift that fills with zeros, rotation preserves all bits by moving the bits that would be shifted out from the left end to the right end. The operation combines a left shift of n positions with a right shift of (32-n) positions using bitwise OR, effectively wrapping the shifted-out bits around. This operation is fundamental to many hash functions used throughout PostgreSQL for distributing hash values more evenly.

## Parameters / Member Variables
- `word`: The 32-bit unsigned integer to be rotated
- `n`: The number of bit positions to rotate to the left (should be between 0-31 for proper behavior)

## Dependencies
- Functions called/Symbols referenced:
  - No external function calls (uses only bitwise operations)
- Called from (representative examples):
  - [TupleHashTableHash_internal](../T/TupleHashTableHash_internal.md) (tuple hashing in executor grouping)
  - [ExecHashGetHashValue](../E/ExecHashGetHashValue.md) (hash value calculation in hash joins)
  - [MemoizeHash_hash](../M/MemoizeHash_hash.md) (memoization hash function)
  - [JsonbHashScalarValue](../J/JsonbHashScalarValue.md) (JSONB scalar value hashing)
  - [hash_multirange](../h/hash_multirange.md) (multirange type hashing)
  - [hash_range](../h/hash_range.md) (range type hashing)
  - [CatalogCacheComputeHashValue](../C/CatalogCacheComputeHashValue.md) (catalog cache hash computation)
  - [guc_name_hash](../g/guc_name_hash.md) (GUC parameter name hashing)
  - rot (common hash function rotation helper)

## Notes and Other Information
- The function is declared as static inline for maximum performance efficiency
- No bounds checking is performed on the rotation count 'n' - caller must ensure n is within valid range (0-31)
- If n is 0, the function returns the original word unchanged
- If n is greater than 31, the behavior follows standard C shift semantics (typically modulo 32)
- Extensively used throughout PostgreSQL's hash infrastructure for improving hash distribution
- The rotation operation is reversible using pg_rotate_right32 with the same count
- Critical component in PostgreSQL's hash functions for data types, catalog caching, and executor operations