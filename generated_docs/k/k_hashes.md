# k_hashes

## Location
[src/backend/lib/bloomfilter.c:250-287](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/bloomfilter.c#L250-L287)

## Overview
The `k_hashes` function generates k hash values for a given element using enhanced double hashing to support up to MAX_HASH_FUNCS hash functions with only two independent hash computations.

## Definition
```c
static void k_hashes(bloom_filter *filter, uint32 *hashes, unsigned char *elem, size_t len)
```

## Detailed Description
This function implements enhanced double hashing to generate multiple hash values from a single element. It uses only two real independent hash functions (derived from a single 64-bit hash) to create up to MAX_HASH_FUNCS (10) hash values. The approach uses `hash_any_extended` with the filter's seed to generate a 64-bit hash, then splits it into two 32-bit values (x and y).

The enhanced double hashing formula is: h_i(x) = (h1(x) + i * h2(x)) mod m, where i ranges from 0 to k-1. This method is preferred over classic double hashing because it avoids collision issues when using power-of-two sized bitsets, as detailed in Dillinger & Manolios research.

The function stores all k hash values in the caller-provided array, with each hash value representing a bit position in the bloom filter bitset.

## Parameters / Member Variables
- `filter`: Pointer to the bloom filter structure containing configuration (bloom_filter *)
- `hashes`: Array to be filled with k hash values (uint32 *)
- `elem`: Pointer to the element data to be hashed (unsigned char *)
- `len`: Length of the element data in bytes (size_t)

## Dependencies
- Functions called/Symbols referenced:
  - [bloom_filter](../b/bloom_filter.md) (struct type)
  - [hash_any_extended](../h/hash_any_extended.md) (PostgreSQL hash function)
  - [DatumGetUInt64](../D/DatumGetUInt64.md) (PostgreSQL type conversion macro)
  - [mod_m](../m/mod_m.md) (modulo calculation for power-of-two values)
- Called from:
  - [bloom_filter](../b/bloom_filter.md) (at src/backend/lib/bloomfilter.c:56)
  - [bloom_add_element](../b/bloom_add_element.md) (at src/backend/lib/bloomfilter.c:140)
  - [bloom_lacks_element](../b/bloom_lacks_element.md) (at src/backend/lib/bloomfilter.c:162)

## Notes and Other Information
- Uses enhanced double hashing instead of classic double hashing to avoid collision issues with power-of-two bitsets
- Generates up to 10 hash functions from only 2 independent hash computations for efficiency
- The seed value from the filter ensures different hash distributions across different bloom filter instances
- All hash values are constrained using mod_m to fit within the bitset size
- The algorithm progressively modifies x and y values to generate subsequent hash functions: x += y, y += i