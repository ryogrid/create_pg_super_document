# hashnameextended

## Location
src/backend/access/hash/hashfunc.c: 258 - 266

## Overview
Computes a 64-bit extended hash value for a PostgreSQL Name data type using a provided seed parameter.

## Definition


## Detailed Description
This function is the extended version of hashname that accepts an additional seed parameter for hash computation. It generates a seeded hash value for a PostgreSQL Name data type by extracting the string content and passing it to hash_any_extended with the provided seed. This allows for hash table implementations that require seeded hashing for better distribution or security purposes.

## Parameters / Member Variables
- : The Name data type argument to be hashed
- : The 64-bit seed value for hash computation

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NAME: Extract Name argument from function call
  - PG_GETARG_INT64: Extract int64 seed argument from function call
  - NameStr: Macro to extract string from Name data type
  - hash_any_extended: Generic extended hash function for binary data with seed
  - strlen: Calculate string length
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- Part of PostgreSQL's hash index infrastructure for Name data types
- Extended version of hashname that supports seeded hashing
- Uses strlen to determine the actual string length, ignoring null padding in the Name type
- Provides better hash distribution and security through seed-based hashing
- The hash only includes the actual string content, not the padding
- Commonly used for seeded hashing of database object identifiers
- Located in src/backend/access/hash/hashfunc.c:258-266