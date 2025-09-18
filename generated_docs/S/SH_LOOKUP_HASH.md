# SH_LOOKUP_HASH

## Location
[src/include/lib/simplehash.h:847-856](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/simplehash.h#L847-L856)

## Overview
A macro that defines the public hash table lookup function name for pre-computed hash values using the SH_MAKE_NAME naming convention for PostgreSQL's generic simple hash table implementation.

## Definition


Function signature (after macro expansion):


## Detailed Description
SH_LOOKUP_HASH is a macro that expands to create a function name for the public hash table lookup operation when the hash value is already computed. This is part of PostgreSQL's generic simple hash table implementation that uses C macros to generate type-specific hash table functions.

The generated function provides an optimized public interface for hash table lookups when the caller already has a computed hash value:
1. Directly delegates to SH_LOOKUP_HASH_INTERNAL with the pre-computed hash
2. Avoids the overhead of recomputing the hash value (unlike SH_LOOKUP)
3. Returns a pointer to the found entry, or NULL if the key is not present

This function is useful for performance-critical scenarios where the hash value has already been computed (e.g., during bulk operations or when the same key is being looked up multiple times).

## Parameters / Member Variables
- : Pointer to the hash table structure
- : The key to search for in the hash table
- hash: hash table empty: Pre-calculated hash value for the key

## Dependencies
- Functions called/Symbols referenced:
  - SH_MAKE_NAME (for name generation)
  - [SH_LOOKUP_HASH_INTERNAL](SH_LOOKUP_HASH_INTERNAL.md) (performs the actual lookup)
- Called from (representative examples):
  - Performance-critical PostgreSQL subsystems with pre-computed hash values
  - Bulk operations that compute hash once and reuse it

## Notes and Other Information
- This is an optimized public interface for hash table lookups when the hash is already available
- Avoids redundant hash computation, making it more efficient than SH_LOOKUP for certain use cases
- Returns NULL if the key is not found in the hash table
- Part of the generic simple hash table implementation that generates type-specific functions
- The SH_SCOPE macro controls the function's visibility (static, extern, etc.)
- Useful for scenarios where hash values are cached or computed in batch