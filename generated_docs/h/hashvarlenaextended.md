# hashvarlenaextended

## Location
src/backend/access/hash/hashfunc.c: 398 - 410

## Overview
An extended version of hashvarlena that supports seed-based hashing for variable-length data types.

## Definition


## Detailed Description
The hashvarlenaextended function is the extended variant of hashvarlena that accepts an additional seed parameter for hash computation. It provides the same direct hashing capability for variable-length data types where all bits are significant, but allows for seed-based hash value generation. This enables the creation of multiple independent hash functions from the same input data, which is valuable for advanced hash table implementations and algorithms requiring hash function families.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  -  (arg 0): The varlena value to be hashed (struct varlena*)
  -  (arg 1): 64-bit seed value for hash computation (int64)

## Dependencies
- Functions called/Symbols referenced:
  - varlena
  - PG_GETARG_VARLENA_PP
  - hash_any_extended
  - PG_GETARG_INT64

- Called from (representative examples):
  - No direct references found

## Notes and Other Information
- Extends hashvarlena with seed-based hashing capability
- Maintains the same direct hashing approach without transformations
- Uses hash_any_extended to incorporate the seed parameter into hash computation
- Suitable for hash table resizing and algorithms requiring multiple hash functions
- Located at src/backend/access/hash/hashfunc.c:398-410