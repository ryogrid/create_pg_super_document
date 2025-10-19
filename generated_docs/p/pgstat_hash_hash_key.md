# pgstat_hash_hash_key

## Location
[src/include/utils/pgstat_internal.h:796-808](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/pgstat_internal.h#L796-L808)

## Overview
A static inline hash function that generates hash values for PgStat_HashKey entries used in PostgreSQL's statistics system hash tables.

## Definition
```c
static inline uint32
pgstat_hash_hash_key(const void *d, size_t size, void *arg)
```

## Detailed Description
This function serves as a hash value generator for dshash and simplehash hashtables used in PostgreSQL's statistics collection system. It takes a PgStat_HashKey structure and generates a 32-bit hash value using the fasthash32 algorithm. The function is designed to work with hash table implementations that require a hash callback function, providing efficient distribution of keys across hash buckets.

## Parameters / Member Variables
- `d`: Pointer to the PgStat_HashKey structure to hash
- `size`: Size of the structure being hashed (must be sizeof(PgStat_HashKey))
- `arg`: Additional argument (must be NULL, not used in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - [PgStat_HashKey](../P/PgStat_HashKey.md) (type referenced for size validation)
  - [fasthash32](../f/fasthash32.md) (fast hash algorithm function)
  - Assert (macro for debug assertions)
- Called from (representative examples):
  - SH_HASH_KEY macro in pgstat.c
  - SH_HASH_KEY macro in pgstat_shmem.c
  - SH_DECLARE macro in pgstat_shmem.c

## Notes and Other Information
- The function includes debug assertions to ensure the size parameter matches sizeof(PgStat_HashKey) and that the arg parameter is NULL
- Uses fasthash32 with a seed value of 0 for consistent hash value generation
- Returns a 32-bit unsigned integer hash value
- Designed as a static inline function for performance optimization in hash table operations
- Part of the PostgreSQL statistics collection infrastructure for efficient key distribution in shared memory hash tables

## Simplified Source

```c
static inline uint32
pgstat_hash_hash_key(const void *d, size_t size, void *arg)
{
    // Generate hash value for PgStat_HashKey using fasthash32
    return fasthash32((const char *) d, size, 0);
}
```