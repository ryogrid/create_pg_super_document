# hashoidvector

## Location
[src/backend/access/hash/hashfunc.c:232-239](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashfunc.c#L232-L239)

## Overview
Computes a hash value for an oidvector data structure containing an array of PostgreSQL object identifiers (OIDs).

## Definition


## Detailed Description
This function generates a hash value for an oidvector by hashing its entire array of OID values. The oidvector is a PostgreSQL data type that stores a vector of object identifiers, commonly used in system catalogs. The function calculates the hash by treating the OID array as raw binary data and passing it to the generic hash_any function.

## Parameters / Member Variables
- : Pointer to the oidvector structure to be hashed

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINTER: Extract pointer argument from function call
  - oidvector: PostgreSQL data type for OID vectors
  - [hash_any](hash_any.md): Generic hash function for binary data
- Called from (representative examples):
  - [oidvectorhashfast](../o/oidvectorhashfast.md): Fast hash function used in catalog cache (src/backend/utils/cache/catcache.c:269)

## Notes and Other Information
- Part of PostgreSQL's hash index infrastructure for oidvector data types
- Uses the dim1 field of oidvector to determine the number of OIDs to hash
- Hashes the entire values array as a contiguous block of memory
- Commonly used in system catalog operations where oidvectors need to be indexed or cached
- Located in src/backend/access/hash/hashfunc.c:232-239