# HASHHDR

## Location
[src/backend/utils/hash/dynahash.c:168-209](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/hash/dynahash.c#L168-L209)

## Overview
HASHHDR is a header structure for PostgreSQL's dynamic hash tables that contains all changeable information and configuration data for the hash table.

## Definition


## Detailed Description
HASHHDR serves as the central control structure for PostgreSQL's dynamic hash tables. In shared-memory hash tables, HASHHDR resides in shared memory while each backend maintains a local HTAB struct. The structure is designed to support both shared and non-shared hash tables, with special considerations for high-concurrency scenarios.

The structure uses an array of freelists (FreeListData) to reduce contention in high-concurrency environments. Each freelist has its own mutex and entry count, allowing for independent operation while supporting cross-freelist scavenging when necessary.

## Parameters / Member Variables
- : Array of freelists for managing free hash table elements, each with independent mutex for concurrency
- : Directory size - number of directory entries (changeable except in partitioned tables)
- : Number of allocated segments, always less than or equal to dsize
- : ID of the maximum bucket currently in use
- : Bit mask for modulo operations across the entire table
- : Bit mask for modulo operations into the lower half of the table
- : Length of hash keys in bytes (fixed at creation)
- : Total size of user elements in bytes (fixed at creation)  
- : Number of partitions, must be power of 2, or 0 for unpartitioned tables
- : Maximum directory size limit for fixed-size directories
- : Segment size, must be a power of 2 (fixed at creation)
- : Segment shift value, calculated as log2(ssize)
- : Number of entries to allocate in a single allocation operation
- : Statistics counter for hash table accesses (when HASH_STATISTICS enabled)
- : Statistics counter for hash collisions (when HASH_STATISTICS enabled)

## Dependencies
- Functions called/Symbols referenced:
  - NUM_FREELISTS (constant: 32)
  - FreeListData (structure containing mutex, nentries, and freeList)
- Called from (representative examples):
  - ShmemInitHash
  - [HTAB](HTAB.md)
  - [hash_create](../h/hash_create.md)
  - [hash_estimate_size](../h/hash_estimate_size.md)
  - [hash_get_shared_size](../h/hash_get_shared_size.md)
  - [element_alloc](../e/element_alloc.md)

## Notes and Other Information
- In shared-memory configurations, HASHHDR exists in shared memory while HTAB structures remain local to each backend
- For unpartitioned tables, only freeList[0] is used and its spinlock is bypassed
- The dsize field cannot change in shared tables, even if they are unpartitioned
- Statistics collection doesn't use mutex protection, so counts may be slightly corrupted in partitioned tables
- The structure is defined at src/backend/utils/hash/dynahash.c:168-209