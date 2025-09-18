# hash_estimate_size

## Location
src/backend/utils/hash/dynahash.c: 784 - 830

## Overview
Estimates the memory footprint required for a shared memory hashtable with a given number of entries and entry size.

## Definition
```c
Size hash_estimate_size(long num_entries, Size entrysize)
```

## Detailed Description
The `hash_estimate_size` function calculates the total memory requirement for a PostgreSQL hash table that will be stored in shared memory. It provides accurate size estimates by accounting for all major components: the hash header structure, directory entries, segments, and the actual hash elements. The function assumes default values for all hash structure parameters and is specifically designed for shared memory usage estimation, excluding the local HTAB structure.

The calculation considers the dynamic growth patterns of hash tables, estimating the number of buckets as the next power of two, computing required segments and directory entries, and accounting for element allocation in groups as determined by the choose_nelem_alloc function.

## Parameters / Member Variables
- `num_entries`: Expected number of entries the hash table will contain
- `entrysize`: Size in bytes of each hash table entry

## Dependencies
- Functions called/Symbols referenced:
  - [next_pow2_long](../n/next_pow2_long.md) (calculates next power of two for bucket and segment sizing)
  - [choose_nelem_alloc](../c/choose_nelem_alloc.md) (determines optimal element allocation group size)
  - [add_size](../a/add_size.md), mul_size (safe arithmetic operations for size calculations)
  - MAXALIGN (aligns sizes to proper boundaries)
- Called from (representative examples):
  - [BufTableShmemSize](../B/BufTableShmemSize.md) (buffer table shared memory sizing)
  - [LockShmemSize](../L/LockShmemSize.md) (lock manager shared memory sizing)
  - [CalculateShmemSize](../C/CalculateShmemSize.md) (overall shared memory calculation)
  - [PredicateLockShmemSize](../P/PredicateLockShmemSize.md) (predicate lock shared memory sizing)

## Notes and Other Information
- Designed specifically for shared memory hashtables - does not count local HTAB structure
- Assumes all hash structure parameters have default values (DEF_SEGSIZE, DEF_DIRSIZE)
- Uses safe arithmetic functions to prevent overflow in size calculations
- Elements are allocated in groups, not individually, for efficiency
- Critical for PostgreSQL shared memory management and sizing decisions
- All size calculations include proper alignment requirements