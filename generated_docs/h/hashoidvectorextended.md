# hashoidvectorextended

## Location
[src/backend/access/hash/hashfunc.c:240-249](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashfunc.c#L240-L249)

## Overview
Computes a 64-bit extended hash value for an oidvector data structure using a provided seed parameter.

## Definition

```c
Datum
hashoidvectorextended(PG_FUNCTION_ARGS)
```
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
  - [hash_any_extended](hash_any_extended.md): Generic extended hash function for binary data with seed
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- Part of PostgreSQL's hash index infrastructure for oidvector data types
- Extended version of hashoidvector that supports seeded hashing
- Uses the dim1 field of oidvector to determine the number of OIDs to hash
- Hashes the entire values array as a contiguous block of memory with the provided seed
- Provides better hash distribution and security through seed-based hashing
- Located in src/backend/access/hash/hashfunc.c:240-249

## Simplified Source
```c
Datum hashoidvectorextended(PG_FUNCTION_ARGS) {
    oidvector *key = (oidvector *) PG_GETARG_POINTER(0);

    // Hash the entire array of OIDs with the provided seed
    return hash_any_extended((unsigned char *) key->values,
                           key->dim1 * sizeof(Oid),
                           PG_GETARG_INT64(1));
}
```