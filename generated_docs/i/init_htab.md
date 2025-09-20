# init_htab

## Location
[src/backend/utils/hash/dynahash.c:690-783](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/hash/dynahash.c#L690-L783)

## Overview
Initializes a hash table by computing derived fields of the hash control structure and building the initial directory/segment arrays.

## Definition

```c
static bool
init_htab(HTAB *hashp, long nelem)
```
## Detailed Description
The  function is a static helper function that performs the crucial initialization work for PostgreSQL's dynamic hash tables. It calculates the optimal number of buckets based on the expected number of elements, sets up the directory structure for managing segments, and allocates the initial memory segments needed for hash table operations.

The function ensures that partitioned hash tables have proper mutex initialization, computes bucket counts as the next power of two greater than the expected element count, and maintains partition independence by ensuring bucket count meets minimum requirements relative to partition count. It also handles the allocation of the directory structure and initial segments, setting up the foundation for dynamic hash table growth.

## Parameters / Member Variables
- : Pointer to the HTAB structure representing the hash table being initialized
- : Expected number of elements the hash table will contain, used for sizing calculations

## Dependencies
- Functions called/Symbols referenced:
  - IS_PARTITIONED (macro to check if hash table is partitioned)
  - SpinLockInit (initializes spinlock mutexes for partitioned tables)
  - [next_pow2_int](../n/next_pow2_int.md) (calculates next power of two for bucket sizing)
  - [seg_alloc](../s/seg_alloc.md) (allocates memory segments for the hash table)
  - [choose_nelem_alloc](../c/choose_nelem_alloc.md) (determines optimal element allocation count)
- Called from (representative examples):
  - [hash_create](../h/hash_create.md) (during hash table creation process)

## Notes and Other Information
- This is a static function, only accessible within dynahash.c
- Returns false if memory allocation fails during initialization
- For partitioned tables, ensures nbuckets >= num_partitions to maintain partition independence
- Uses a load factor of 1 for initial bucket count estimation
- The function includes debug output when HASH_DEBUG is enabled
- Critical for proper hash table setup - failure here prevents hash table creation