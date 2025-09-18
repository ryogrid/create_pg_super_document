# hashoidvectorextended

## Location
src/backend/access/hash/hashfunc.c: 240 - 249

## Overview
Computes a 64-bit extended hash value for an oidvector data structure using a provided seed parameter.

## Definition


## Detailed Description
This function is the extended version of hashoidvector that accepts an additional seed parameter for hash computation. It generates a seeded hash value for an oidvector by hashing its entire array of OID values using the generic hash_any_extended function. This allows for hash table implementations that require seeded hashing for better distribution or security.

## Parameters / Member Variables
- : Pointer to the oidvector structure to be hashed
- : The 64-bit seed value for hash computation

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINTER: Extract pointer argument from function call
  - PG_GETARG_INT64: Extract int64 seed argument from function call
  - oidvector: PostgreSQL data type for OID vectors
  - hash_any_extended: Generic extended hash function for binary data with seed
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- Part of PostgreSQL's hash index infrastructure for oidvector data types
- Extended version of hashoidvector that supports seeded hashing
- Uses the dim1 field of oidvector to determine the number of OIDs to hash
- Hashes the entire values array as a contiguous block of memory with the provided seed
- Provides better hash distribution and security through seed-based hashing
- Located in src/backend/access/hash/hashfunc.c:240-249