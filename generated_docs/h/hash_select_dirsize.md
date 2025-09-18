# hash_select_dirsize

## Location
src/backend/utils/hash/dynahash.c: 831 - 854

## Overview
Selects an appropriate directory size for a shared memory hashtable with a given maximum number of entries.

## Definition
```c
long hash_select_dirsize(long num_entries)
```

## Detailed Description
The `hash_select_dirsize` function determines the optimal directory size for hash tables stored in shared memory. Since shared memory hash tables cannot dynamically expand their directories like regular hash tables, this function must accurately predict the directory size needed to accommodate all expected segments. It calculates the number of buckets and segments required, then determines the directory size by finding the next power of two that can hold all necessary segment pointers.

The function mirrors the logic used in `init_htab` to ensure consistency between size estimation and actual initialization, using the same default parameters and sizing algorithms.

## Parameters / Member Variables
- `num_entries`: Expected maximum number of entries the hash table will contain during its lifetime

## Dependencies
- Functions called/Symbols referenced:
  - next_pow2_long (calculates next power of two for bucket and segment counts)
  - DEF_SEGSIZE (default segment size constant)
  - DEF_DIRSIZE (default directory size constant)
- Called from (representative examples):
  - ShmemInitHash (shared memory hash table initialization)

## Notes and Other Information
- Specifically designed for shared memory hash tables with fixed directory sizes
- Must agree with the behavior of init_htab() to ensure consistency
- Uses the same default parameters (DEF_SEGSIZE, DEF_DIRSIZE) as other hash functions
- Returns directory size as a power of two to accommodate segment doubling during growth
- Critical for preventing directory overflow in shared memory environments
- The directory size cannot be changed after hash table creation in shared memory