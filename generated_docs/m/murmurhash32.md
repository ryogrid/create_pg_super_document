# murmurhash32

## Location
[src/include/common/hashfn.h:92-105](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/hashfn.h#L92-L105)

## Overview
A simple inline implementation of the MurmurHash hash function optimized for 32-bit integer input, designed for high-performance hashing operations.

## Definition

```c
static inline uint32
murmurhash32(uint32 data)
```
## Detailed Description
The `murmurhash32` function implements a simplified version of the MurmurHash algorithm specifically designed for hashing 32-bit integers. It applies a series of bitwise operations (XOR shifts and multiplication with carefully chosen constants) to distribute the bits of the input data uniformly across the output space. This implementation prioritizes performance by being declared as an inline static function, eliminating function call overhead while providing good hash distribution properties.

The algorithm uses two magic constants (0x85ebca6b and 0xc2b2ae35) that are specifically chosen to provide good avalanche properties, meaning small changes in input produce large changes in output. The function performs three XOR-shift operations interspersed with two multiplications to ensure thorough bit mixing.

## Parameters / Member Variables
- `data`: The 32-bit unsigned integer value to be hashed

## Dependencies
- Functions called/Symbols referenced: None (pure computational function)
- Called from (representative examples):
  - [BuildTupleHashTableExt](../B/BuildTupleHashTableExt.md) (src/backend/executor/execGrouping.c:203)
  - [TupleHashTableHash_internal](../T/TupleHashTableHash_internal.md) (src/backend/executor/execGrouping.c:482)
  - [MemoizeHash_hash](../M/MemoizeHash_hash.md) (src/backend/executor/nodeMemoize.c:211)
  - [charhashfast](../c/charhashfast.md) (src/backend/utils/cache/catcache.c:199)
  - [int2hashfast](../i/int2hashfast.md) (src/backend/utils/cache/catcache.c:228)
  - [int4hashfast](../i/int4hashfast.md) (src/backend/utils/cache/catcache.c:240)
  - [hash_resource_elem](../h/hash_resource_elem.md) (src/backend/utils/resowner/resowner.c:229)

## Notes and Other Information
- This is a performance-optimized variant of MurmurHash designed specifically for 32-bit integer inputs
- The function is declared as `static inline` to eliminate function call overhead
- The magic constants used (0x85ebca6b and 0xc2b2ae35) are part of the MurmurHash specification and provide good bit mixing properties
- Widely used throughout PostgreSQL for fast hashing of integer values in hash tables, catalogs, and other data structures requiring quick hash computations
- The algorithm provides good hash distribution while being computationally lightweight

## Simplified Source

```c
static inline uint32
murmurhash32(uint32 data)
{
    uint32 h = data;

    // Apply bit mixing with XOR shifts and magic constants
    h ^= h >> 16;           // Right shift and XOR
    h *= 0x85ebca6b;        // Multiply by first magic constant
    h ^= h >> 13;           // Right shift and XOR
    h *= 0xc2b2ae35;        // Multiply by second magic constant
    h ^= h >> 16;           // Final right shift and XOR

    return h;
}
```