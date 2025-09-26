# int4hashfast

## Location
src/backend/utils/cache/catcache.c: 238 - 243

## Overview
A fast hash function for 32-bit integers used in PostgreSQL's catalog cache system to generate hash values for INT4OID type keys.

## Definition
```c
static uint32 int4hashfast(Datum datum)
```

## Detailed Description
int4hashfast is a specialized hash function designed for high-performance hashing of 32-bit integer values in the catalog cache system. It extracts a 32-bit integer from a Datum and computes its hash using the MurmurHash32 algorithm. This function is part of PostgreSQL's catalog cache optimization, providing fast hash computation for cache key distribution.

The function is used specifically by the catalog cache system to efficiently hash cache keys of INT4 type and various REG* types, avoiding the overhead of the full PostgreSQL function call mechanism and ensuring good hash distribution for cache performance.

## Parameters / Member Variables
- `datum`: Datum containing a 32-bit integer value to hash

## Dependencies
- Functions called/Symbols referenced:
  - murmurhash32 (hash function for generating hash values)
  - DatumGetInt32 (macro for extracting int32 from Datum)
- Called from (representative examples):
  - GetCCHashEqFuncs (assigned as hash function for INT4OID and various REG* types)

## Notes and Other Information
- This function is static and only used within catcache.c
- Used for multiple PostgreSQL types: INT4OID and all REG* types (REGPROCOID, REGPROCEDUREOID, REGOPEROID, etc.) since they are all internally represented as 32-bit integers  
- Part of the catalog cache optimization system that provides fast hash functions for commonly used data types
- Uses MurmurHash32 algorithm which provides good hash distribution and performance
- The function bypasses the normal PostgreSQL function call overhead for better performance in cache operations
- Returns a 32-bit hash value for use in hash table indexing