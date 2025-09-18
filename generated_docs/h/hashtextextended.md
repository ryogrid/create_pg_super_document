# hashtextextended

## Location
src/backend/access/hash/hashfunc.c: 323 - 382

## Overview
An extended version of the hashtext function that supports seed-based hashing for text data types with collation awareness.

## Definition


## Detailed Description
The hashtextextended function is the extended variant of hashtext that accepts an additional seed parameter for hash computation. Like hashtext, it handles collation-aware hashing by determining the appropriate locale and choosing between direct hashing for deterministic locales and transformation-based hashing for non-deterministic locales. The seed parameter allows for creating different hash values from the same input, which is useful for hash table resizing and hash-based algorithms that require multiple hash functions.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  -  (arg 0): The text value to be hashed (text*)
  -  (arg 1): 64-bit seed value for hash computation (int64)
  - Implicit collation from PG_GET_COLLATION()

## Dependencies
- Functions called/Symbols referenced:
  - PG_GET_COLLATION
  - pg_locale_t
  - lc_collate_is_c
  - pg_newlocale_from_collation
  - pg_locale_deterministic
  - hash_any_extended
  - PG_GETARG_INT64
  - pg_strnxfrm

- Called from (representative examples):
  - No direct references found

## Notes and Other Information
- Provides seed-based hashing capability for advanced hash table operations
- Maintains the same collation-aware behavior as hashtext
- Uses hash_any_extended instead of hash_any to incorporate the seed parameter
- Follows identical error handling and memory management patterns as hashtext
- Located at src/backend/access/hash/hashfunc.c:323-382