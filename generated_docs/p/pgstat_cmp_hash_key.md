# pgstat_cmp_hash_key

## Location
src/include/utils/pgstat_internal.h: 789 - 795

## Overview
A static inline comparison function used by hash tables to compare PgStat_HashKey entries for equality in PostgreSQL's statistics system.

## Definition


## Detailed Description
This function serves as a key comparison helper for dshash and simplehash hashtables used in PostgreSQL's statistics collection system. It performs a binary comparison between two PgStat_HashKey structures to determine if they are equal. The function is designed to work with hash table implementations that require a comparison callback function, returning 0 for equal keys and non-zero for different keys.

## Parameters / Member Variables
- : Pointer to the first PgStat_HashKey structure to compare
- : Pointer to the second PgStat_HashKey structure to compare  
- : Size of the structures being compared (must be sizeof(PgStat_HashKey))
- : Additional argument (must be NULL, not used in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - PgStat_HashKey (type referenced for size validation)
  - memcmp (standard C library function for memory comparison)
  - Assert (macro for debug assertions)
- Called from (representative examples):
  - SH_EQUAL macro in pgstat.c
  - SH_EQUAL macro in pgstat_shmem.c
  - SH_DECLARE macro in pgstat_shmem.c

## Notes and Other Information
- The function includes debug assertions to ensure the size parameter matches sizeof(PgStat_HashKey) and that the arg parameter is NULL
- Uses memcmp for efficient binary comparison of the entire key structure
- Designed as a static inline function for performance optimization in hash table operations
- Part of the PostgreSQL statistics collection infrastructure for managing statistical data in shared memory hash tables