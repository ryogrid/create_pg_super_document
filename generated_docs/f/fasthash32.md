# fasthash32

## Location
src/include/common/hashfn_unstable.h: 382 - 390

## Overview
A 32-bit variant of the fasthash64 function that provides the same high-performance hashing algorithm but returns a reduced 32-bit hash value suitable for applications requiring smaller hash codes.

## Definition


## Detailed Description
The fasthash32 function is a wrapper around fasthash64 that provides a 32-bit hash output while maintaining the same underlying algorithm and performance characteristics. It leverages the full 64-bit fasthash64 computation and then reduces the result to 32 bits using the fasthash_reduce32 function, ensuring good distribution properties are preserved in the smaller output space.

This function is particularly useful in scenarios where memory constraints or hash table sizes make 32-bit hash codes more appropriate, while still benefiting from the quality and speed of the fasthash algorithm.

## Parameters / Member Variables
- : Pointer to the input data to be hashed (const char array)
- : Length of the input data in bytes; also used to modify the internal seed in fasthash64
- : Initial seed value for the hash function (can be zero for default behavior)

## Dependencies
- Functions called/Symbols referenced:
  - fasthash64 (core 64-bit hash computation)
  - fasthash_reduce32 (reduction function to convert 64-bit to 32-bit hash)
- Called from (representative examples):
  - pgstat_hash_hash_key (PostgreSQL statistics hash key function)

## Notes and Other Information
- Declared as static inline for optimal performance in header file src/include/common/hashfn_unstable.h
- Inherits all the algorithmic properties of fasthash64, including length-dependent seed modification
- The fasthash_reduce32 function ensures that the 32-bit reduction maintains good hash distribution
- Part of PostgreSQL's unstable hash function family, meaning hash values may change between versions
- Provides a convenient interface for code that needs 32-bit hashes while leveraging the full quality of the 64-bit algorithm
- Commonly used in PostgreSQL's internal hash tables and statistics systems where 32-bit hashes are sufficient