# SH_LOOKUP_HASH_INTERNAL

## Location
[src/include/lib/simplehash.h:800-833](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/simplehash.h#L800-L833)

## Overview
A macro that defines the internal hash table lookup function name using the SH_MAKE_NAME naming convention for PostgreSQL's generic simple hash table implementation.

## Definition


Function signature (after macro expansion):


## Detailed Description
SH_LOOKUP_HASH_INTERNAL is a macro that expands to create a function name for the internal hash table lookup operation. This is part of PostgreSQL's generic simple hash table implementation (simplehash.h) that uses C macros to generate type-specific hash table functions. The actual function performs a linear probe hash table lookup using an already-computed hash value.

The generated function implements the core lookup algorithm:
1. Calculates the initial bucket position using the hash value
2. Performs linear probing to find the entry with matching key
3. Returns the entry pointer if found, or NULL if not present
4. Handles hash collision resolution through linear probing

This is an internal static inline function designed to be called by the public SH_LOOKUP and SH_LOOKUP_HASH functions, providing the actual lookup implementation that can be reliably inlined.

## Parameters / Member Variables
- : Pointer to the hash table structure
- : The key to search for in the hash table
- hash: hash table empty: Pre-calculated hash value for the key (avoids recomputation)

## Dependencies
- Functions called/Symbols referenced:
  - SH_MAKE_NAME (for name generation)
  - [SH_INITIAL_BUCKET](SH_INITIAL_BUCKET.md) (calculates starting bucket)
  - SH_COMPARE_KEYS (compares keys for equality)
  - [SH_NEXT](SH_NEXT.md) (moves to next bucket in probe sequence)
- Called from (representative examples):
  - [SH_LOOKUP](SH_LOOKUP.md) (public lookup function)
  - [SH_LOOKUP_HASH](SH_LOOKUP_HASH.md) (public lookup with pre-computed hash)

## Notes and Other Information
- This is a static inline function to ensure reliable inlining even when SH_SCOPE is extern
- Part of the generic simple hash table implementation that generates type-specific functions
- Uses linear probing for collision resolution
- Contains a TODO comment about potential optimization using distance-from-optimal to stop searches early
- The function is designed to be highly optimized for performance-critical PostgreSQL operations