# bloom_filter

## Location
[src/backend/lib/bloomfilter.c:44-86](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/bloomfilter.c#L44-L86)

## Overview
The `bloom_filter` struct is the core data structure for implementing Bloom filters in PostgreSQL, providing space-efficient probabilistic membership testing with configurable false positive rates.

## Definition
```c
struct bloom_filter
{
    /* K hash functions are used, seeded by caller's seed */
    int         k_hash_funcs;
    uint64      seed;
    /* m is bitset size, in bits.  Must be a power of two <= 2^32.  */
    uint64      m;
    unsigned char bitset[FLEXIBLE_ARRAY_MEMBER];
};
```

## Detailed Description
The `bloom_filter` struct represents a Bloom filter data structure used for probabilistic membership testing in PostgreSQL. A Bloom filter is a space-efficient probabilistic data structure that can test whether an element is in a set, with the possibility of false positives but no false negatives. The structure uses k independent hash functions to map elements to bit positions in a bitset array. When an element is added, all k hash positions are set to 1. When testing for membership, all k positions must be 1 for the element to be considered "possibly in the set."

The implementation uses enhanced double hashing for generating the k hash functions from a single seed, allowing for efficient computation while maintaining good distribution properties. The bitset size must be a power of two to enable fast modulo operations using bitwise operations.

## Parameters / Member Variables
- `k_hash_funcs`: The number of hash functions (k) used by the Bloom filter, determining the trade-off between space and false positive rate
- `seed`: The seed value used to generate the k hash functions through enhanced double hashing
- `m`: The size of the bitset in bits, which must be a power of two and not exceed 2^32 for efficient modulo operations
- `bitset`: A flexible array member containing the actual bit array where hash positions are set to indicate element presence

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER
  - [my_bloom_power](../m/my_bloom_power.md)
  - [optimal_k](../o/optimal_k.md)
  - [k_hashes](../k/k_hashes.md)
  - [mod_m](../m/mod_m.md)
- Called from (representative examples):
  - [bloom_create](bloom_create.md)
  - [bloom_free](bloom_free.md)
  - [bloom_add_element](bloom_add_element.md)
  - [bloom_lacks_element](bloom_lacks_element.md)
  - [bloom_prop_bits_set](bloom_prop_bits_set.md)
  - roles_list_append (in ACL system)
  - roles_is_member_of (in ACL system)

## Notes and Other Information
- The structure is designed with a flexible array member to minimize memory overhead and allow variable-sized bitsets
- The bitset size restriction to powers of two enables the use of fast bitwise AND operations instead of expensive modulo operations
- The implementation is used both in core PostgreSQL functionality (such as the ACL system for role membership testing) and in test modules
- The k_hash_funcs value is typically calculated using the optimal_k function based on the expected number of elements and desired false positive rate
- The bitset size (m) is determined using the my_bloom_power function to ensure it is a power of two
- Memory layout is optimized with the bitset immediately following the fixed-size members, reducing cache misses and memory fragmentation