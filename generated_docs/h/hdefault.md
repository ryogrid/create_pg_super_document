# hdefault

## Location
src/backend/utils/hash/dynahash.c: 630 - 656

## Overview
hdefault is a static function that initializes a hash table's header structure with default parameters and settings.

## Definition


## Detailed Description
hdefault serves as the default initialization function for HASHHDR structures within PostgreSQL's dynamic hash table implementation. This function clears the hash header and sets standard default values for directory size, segment parameters, partitioning, and statistical tracking. It establishes the basic configuration that can later be overridden by specific flags and parameters passed to hash_create. The function ensures that all hash tables start with consistent, well-defined default behavior before any custom configuration is applied.

## Parameters / Member Variables
- : Pointer to the HTAB structure whose header (hctl) needs to be initialized with defaults

## Dependencies
- Functions called/Symbols referenced:
  - [HASHHDR](../H/HASHHDR.md)
  - MemSet
  - DEF_DIRSIZE
  - NO_MAX_DSIZE
  - DEF_SEGSIZE
  - DEF_SEGSIZE_SHIFT
- Called from (representative examples):
  - [hash_create](hash_create.md)
  - MOD

## Notes and Other Information
- This is a static function, only accessible within dynahash.c
- Initializes hash table header with safe, consistent defaults
- Sets dsize to DEF_DIRSIZE for initial directory size
- Configures table as non-partitioned (num_partitions = 0)
- Sets no fixed maximum size (max_dsize = NO_MAX_DSIZE)
- Establishes default segment size and shift parameters
- Conditionally initializes statistics counters when HASH_STATISTICS is enabled
- Essential for ensuring consistent hash table initialization across all use cases
- Located at src/backend/utils/hash/dynahash.c:630-656