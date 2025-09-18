# SH_PREV

## Location
src/include/lib/simplehash.h: 375 - 385

## Overview
Returns the previous bucket before the current bucket in a PostgreSQL simple hash table, handling wraparound for backward linear probing.

## Definition


## Detailed Description
SH_PREV is a macro that generates a function name for moving to the previous bucket within PostgreSQL's simple hash table framework. The function implements backward linear probing by decrementing the current bucket index and wrapping around to the end of the table using bitwise AND with the size mask. Similar to SH_NEXT, it includes an assertion to prevent infinite loops by ensuring the probing doesn't return to the starting element. This function is less commonly used than SH_NEXT but is essential for certain hash table operations that require backward traversal.

## Parameters / Member Variables
- `tb`: Pointer to the hash table structure containing the sizemask
- `curelem`: The current bucket index to move back from
- `startelem`: The starting bucket index (used for wraparound detection)

## Dependencies
- Functions called/Symbols referenced:
  - tb->sizemask (bitmask for efficient wraparound)
  - Assert (assertion macro for debugging)
- Called from (representative examples):
  - SH_INSERT_HASH_INTERNAL (when performing backward probing during insertion)

## Notes and Other Information
- Uses (curelem - 1) & tb->sizemask for efficient wraparound without expensive modulo operations
- The Assert ensures infinite loops are caught during development if the table becomes completely full
- Implements backward linear probing, which is less common than forward probing
- Part of PostgreSQL's templated simple hash table implementation where SH_PREFIX defines the specific hash table type
- Used in specialized scenarios where backward traversal through hash table buckets is required