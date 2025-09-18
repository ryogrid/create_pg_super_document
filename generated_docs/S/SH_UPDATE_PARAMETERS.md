# SH_UPDATE_PARAMETERS

## Location
src/include/lib/simplehash.h: 337 - 356

## Overview
Updates the sizing parameters for a PostgreSQL simple hash table when creating or growing the table, setting size, mask, and growth threshold values.

## Definition


## Detailed Description
SH_UPDATE_PARAMETERS is a macro that generates a function name for updating hash table sizing parameters within PostgreSQL's simple hash table framework. The function computes the optimal hash table size using SH_COMPUTE_SIZE, then updates the hash table structure with the new size, calculates a bitmask for efficient modulo operations, and determines the next growth threshold based on fill factor constraints. This function is essential for maintaining hash table performance by ensuring proper load balancing and efficient bucket addressing.

## Parameters / Member Variables
- `tb`: Pointer to the hash table structure to update
- `newsize`: The requested new size for the hash table

## Dependencies
- Functions called/Symbols referenced:
  - [SH_COMPUTE_SIZE](SH_COMPUTE_SIZE.md) (computes optimal hash table size)
  - SH_MAX_SIZE (maximum allowable hash table size)
  - SH_FILLFACTOR (normal fill factor, typically 0.9)
  - SH_MAX_FILLFACTOR (maximum fill factor, typically 0.98)
- Called from (representative examples):
  - SH_CREATE (when creating new hash tables)
  - [SH_GROW](SH_GROW.md) (when expanding existing hash tables)

## Notes and Other Information
- Sets tb->sizemask to (size - 1) for efficient modulo operations using bitwise AND
- Uses different fill factors depending on whether the table has reached maximum size
- The grow_threshold determines when the hash table needs to be expanded to maintain performance
- Part of PostgreSQL's templated simple hash table implementation where SH_PREFIX defines the specific hash table type
- Critical for maintaining hash table load balance and preventing performance degradation due to excessive collisions