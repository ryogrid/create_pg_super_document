# SH_LOOKUP

## Location
src/include/lib/simplehash.h: 834 - 846

## Overview
A macro that defines the public hash table lookup function name using the SH_MAKE_NAME naming convention for PostgreSQL's generic simple hash table implementation.

## Definition


Function signature (after macro expansion):


## Detailed Description
SH_LOOKUP is a macro that expands to create a function name for the public hash table lookup operation. This is part of PostgreSQL's generic simple hash table implementation that uses C macros to generate type-specific hash table functions.

The generated function provides a convenient public interface for hash table lookups by:
1. Computing the hash value for the provided key using SH_HASH_KEY
2. Delegating the actual lookup to SH_LOOKUP_HASH_INTERNAL with the computed hash
3. Returning a pointer to the found entry, or NULL if the key is not present

This function is the primary lookup interface that most clients of the hash table would use when they only have the key and want the system to compute the hash automatically.

## Parameters / Member Variables
- : Pointer to the hash table structure
- : The key to search for in the hash table

## Dependencies
- Functions called/Symbols referenced:
  - SH_MAKE_NAME (for name generation)
  - SH_HASH_KEY (computes hash value for the key)
  - SH_LOOKUP_HASH_INTERNAL (performs the actual lookup)
- Called from (representative examples):
  - Various PostgreSQL subsystems that need to look up entries by key only

## Notes and Other Information
- This is the primary public interface for hash table lookups when only the key is available
- Automatically computes the hash value, making it convenient for most use cases
- Returns NULL if the key is not found in the hash table
- Part of the generic simple hash table implementation that generates type-specific functions
- The SH_SCOPE macro controls the function's visibility (static, extern, etc.)