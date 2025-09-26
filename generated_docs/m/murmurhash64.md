# murmurhash64

## Location
[src/include/common/hashfn.h:106-119](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/hashfn.h#L106-L119)

## Overview
A 64-bit variant of the MurmurHash algorithm optimized for hashing 64-bit integer values, providing high-performance hash computation with good distribution properties.

## Definition
```c
static inline uint64 murmurhash64(uint64 data)
```

## Detailed Description
The `murmurhash64` function implements a 64-bit version of the MurmurHash algorithm specifically designed for hashing 64-bit integers. Similar to its 32-bit counterpart, it applies a series of bitwise XOR-shift operations and multiplications with carefully chosen 64-bit constants to ensure uniform distribution of hash values. The algorithm follows the same pattern as `murmurhash32` but uses 64-bit operations and constants optimized for the larger data size.

The function performs three XOR-shift operations (each shifting by 33 bits) interspersed with two multiplications using magic constants (0xff51afd7ed558ccd and 0xc4ceb9fe1a85ec53) that are specifically chosen for 64-bit hash operations to provide excellent avalanche properties and bit mixing.

## Parameters / Member Variables
- `data`: The 64-bit unsigned integer value to be hashed

## Dependencies
- Functions called/Symbols referenced: None (pure computational function)
- Called from (representative examples):
  - [hash_resource_elem](../h/hash_resource_elem.md) (src/backend/utils/resowner/resowner.c:227)

## Notes and Other Information
- This is the 64-bit variant of the MurmurHash algorithm, complementing `murmurhash32`
- Declared as `static inline` for maximum performance by eliminating function call overhead
- The magic constants (0xff51afd7ed558ccd and 0xc4ceb9fe1a85ec53) are 64-bit specific values from the MurmurHash specification
- Uses 33-bit right shifts, which is optimal for 64-bit hash mixing (approximately half the word size)
- Less frequently used than `murmurhash32` in the PostgreSQL codebase, primarily employed for hashing 64-bit values such as pointers or large integer identifiers
- Provides the same quality hash distribution as the 32-bit version but for 64-bit input space