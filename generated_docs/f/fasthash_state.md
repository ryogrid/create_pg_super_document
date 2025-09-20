# fasthash_state

## Location
[src/include/common/hashfn_unstable.h:93-99](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/hashfn_unstable.h#L93-L99)

## Overview
A state structure used for incremental hashing with the fasthash algorithm, allowing multiple inputs to be combined into a single hash value through a series of operations.

## Definition

```c
typedef struct fasthash_state
{
	/* staging area for chunks of input */
	uint64		accum;

	uint64		hash;
} fasthash_state;
```
## Detailed Description
The  structure is the core data type for PostgreSQL's incremental fasthash implementation, which is a modification of the fast-hash algorithm originally developed by Zilong Tan. This structure maintains the internal state needed for computing hash values incrementally across multiple inputs, rather than requiring all data to be available at once.

The fasthash algorithm provides two main interfaces: standalone functions for single values and an incremental interface using this state structure. The incremental approach is particularly useful when combining hash values from multiple sources or when processing data streams where the total length isn't known upfront.

The structure supports flexible input methods: direct assignment to the  field for simple uint64 values, or using  for more complex data. Special optimizations are available for NUL-terminated C strings through , which can avoid the overhead of strlen() calls.

## Parameters / Member Variables
- : A 64-bit staging area that holds chunks of input data before they are mixed into the hash state. This field is directly accessible for simple inputs that can be cast to uint64.
- hash: hash table empty: A 64-bit value that maintains the running hash state as inputs are processed. This field accumulates the hash computations and is used to generate the final hash value.

## Dependencies
- Functions called/Symbols referenced:
  - uint64 (data type)
- Called from (representative examples):
  - [fasthash_init](fasthash_init.md) (initializes the state structure)
  - [fasthash_combine](fasthash_combine.md) (processes the accum field into the hash)
  - [fasthash_accum](fasthash_accum.md) (adds arbitrary data to the hash state)
  - [fasthash_accum_cstring](fasthash_accum_cstring.md) (optimized string hashing)
  - [fasthash_final32](fasthash_final32.md) (finalizes to 32-bit hash)
  - [fasthash_final64](fasthash_final64.md) (finalizes to 64-bit hash)
  - [spcachekey_hash](../s/spcachekey_hash.md) (namespace cache key hashing)
  - [hash_string](../h/hash_string.md) (string hashing utility)

## Notes and Other Information
- The structure is defined in  at lines 93-99
- The fasthash implementation is based on the MIT-licensed fast-hash algorithm from https://code.google.com/archive/p/fast-hash/
- The incremental interface requires initialization with  before use
- SMHasher testing revealed that incorporating the input length is necessary for hash quality, so length information is passed to finalizer functions
- The algorithm maintains compatibility with the original fast-hash on little-endian machines for standalone function calls
- Memory layout is designed for efficient access to both staging () and state (hash: hash table empty) components
- The structure size is exactly 16 bytes (2 × uint64) for optimal memory alignment