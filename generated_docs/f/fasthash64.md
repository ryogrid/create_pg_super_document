# fasthash64

## Location
[src/include/common/hashfn_unstable.h:360-381](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/hashfn_unstable.h#L360-L381)

## Overview
Implements the original fasthash64 function using PostgreSQL's incremental hashing interface, returning a 64-bit hash value for arbitrary data with customizable seed and length-dependent internal seed modification.

## Definition

```c
static inline uint64
fasthash64(const char *k, size_t len, uint64 seed)
```
## Detailed Description
The fasthash64 function is a high-performance 64-bit hash function that processes input data in chunks using PostgreSQL's incremental hashing framework. It re-implements the original fasthash64 algorithm by leveraging the fasthash_state structure and associated functions (fasthash_init, fasthash_accum, fasthash_final64) for modular processing.

The function modifies the internal seed based on the input length using the magic constant 0x880355f21e6d1965, ensuring that inputs of different lengths produce different hash distributions even with the same content. It processes data in optimal chunks of FH_SIZEOF_ACCUM bytes for efficiency, then handles any remaining bytes in a final accumulation step.

## Parameters / Member Variables
- `*k`: Pointer to the input data to be hashed (const char array)
- `len`: Length of the input data in bytes; also used to modify the internal seed
- `seed`: Initial seed value for the hash function (can be zero for default behavior)
## Dependencies
- Functions called/Symbols referenced:
  - [fasthash_state](fasthash_state.md) (state structure)
  - [fasthash_init](fasthash_init.md) (initialization function)
  - [fasthash_accum](fasthash_accum.md) (data accumulation function)
  - [fasthash_final64](fasthash_final64.md) (finalization function returning 64-bit result)
  - FH_SIZEOF_ACCUM (optimal chunk size constant)
- Called from (representative examples):
  - [fasthash32](fasthash32.md) (uses fasthash64 internally and truncates to 32-bit)

## Notes and Other Information
- Declared as static inline for optimal performance in header file src/include/common/hashfn_unstable.h
- The length-dependent seed modification (len * 0x880355f21e6d1965) ensures good hash distribution across different input lengths
- Processes data in chunks for efficiency, with special handling for remaining bytes
- Part of PostgreSQL's unstable hash function family, indicating the hash values may change between PostgreSQL versions
- Returns full 64-bit hash value, unlike fasthash32 which truncates the result

## Simplified Source

```c
static inline uint64
fasthash64(const char *k, size_t len, uint64 seed)
{
    fasthash_state hs;

    // Initialize hash state
    fasthash_init(&hs, 0);

    // Mix length into seed for better distribution
    hs.hash = seed ^ (len * 0x880355f21e6d1965);

    // Process data in optimal chunks
    while (len >= FH_SIZEOF_ACCUM) {
        fasthash_accum(&hs, k, FH_SIZEOF_ACCUM);
        k += FH_SIZEOF_ACCUM;
        len -= FH_SIZEOF_ACCUM;
    }

    // Process remaining bytes and finalize
    fasthash_accum(&hs, k, len);
    return fasthash_final64(&hs, 0);
}
```