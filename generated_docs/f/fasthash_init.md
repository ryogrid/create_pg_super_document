# fasthash_init

## Location
[src/include/common/hashfn_unstable.h:110-117](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/hashfn_unstable.h#L110-L117)

## Overview
Initializes a fasthash state structure for incremental hashing operations, setting up the initial hash value with an optional seed.

## Definition
```c
static inline void
fasthash_init(fasthash_state *hs, uint64 seed)
```

## Detailed Description
The `fasthash_init` function is the entry point for PostgreSQL's fasthash incremental hashing interface. It prepares a `fasthash_state` structure for accepting input data that will be processed incrementally through subsequent calls to other fasthash functions like `fasthash_combine()` or `fasthash_accum()`.

The function zeroes out the entire state structure using `memset()` and then initializes the hash field with a combination of the provided seed and a magic constant (0x880355f21e6d1965). This magic constant is part of the fasthash algorithm's initialization to ensure good hash distribution properties.

The fasthash algorithm is a modification of the fast-hash implementation originally created by Zilong Tan, adapted for PostgreSQL's needs while maintaining the MIT license terms.

## Parameters / Member Variables
- `hs`: Pointer to a `fasthash_state` structure that will be initialized. This structure contains:
  - `accum`: A staging area for chunks of input data (zeroed by this function)  
  - `hash`: The current hash value (initialized with seed ^ magic constant)
- `seed`: A 64-bit seed value that can be zero. The seed allows for creating different hash families or adding randomization to prevent hash collision attacks.

## Dependencies
- Functions called/Symbols referenced:
  - `memset` (standard C library function)
  - [fasthash_state](fasthash_state.md) (structure type)
- Called from (representative examples):
  - [spcachekey_hash](../s/spcachekey_hash.md) (src/backend/catalog/namespace.c:259)
  - [fasthash64](fasthash64.md) (src/include/common/hashfn_unstable.h:364)
  - [hash_string](../h/hash_string.md) (src/include/common/hashfn_unstable.h:396)

## Notes and Other Information
- This function is declared as `static inline` for performance, as it's a simple initialization routine that benefits from inlining
- The magic constant 0x880355f21e6d1965 is part of the fasthash algorithm design and should not be modified
- After initialization, users typically call `fasthash_combine()` for simple values or `fasthash_accum()` for more complex data, followed by `fasthash_final32()` or `fasthash_final64()` to get the final hash value
- The seed parameter allows creating different hash functions from the same algorithm, which can be useful for hash tables that need to rehash or for security purposes

## Simplified Source

```c
static inline void
fasthash_init(fasthash_state *hs, uint64 seed)
{
    // Zero out the entire state structure
    memset(hs, 0, sizeof(fasthash_state));

    // Initialize hash with seed XORed with magic constant
    hs->hash = seed ^ 0x880355f21e6d1965;
}
```