# SH_NEXT

## Location
src/include/lib/simplehash.h: 364 - 374

## Overview
Returns the next bucket after the current bucket in a PostgreSQL simple hash table, handling wraparound to implement linear probing.

## Definition


## Detailed Description
SH_NEXT is a macro that generates a function name for advancing to the next bucket within PostgreSQL's simple hash table framework. The function implements linear probing by incrementing the current bucket index and wrapping around to the beginning of the table using bitwise AND with the size mask. It includes an assertion to ensure the probing doesn't return to the starting element, which would indicate a full table traversal. This function is essential for collision resolution in the hash table, allowing the search to continue when the ideal bucket is occupied.

## Parameters / Member Variables
- `tb`: Pointer to the hash table structure containing the sizemask
- `curelem`: The current bucket index to advance from
- `startelem`: The starting bucket index (used for wraparound detection)

## Dependencies
- Functions called/Symbols referenced:
  - tb->sizemask (bitmask for efficient wraparound)
  - Assert (assertion macro for debugging)
- Called from (representative examples):
  - [SH_GROW](SH_GROW.md) (when redistributing elements during table growth)
  - [SH_INSERT_HASH_INTERNAL](SH_INSERT_HASH_INTERNAL.md) (when probing for insertion slots)
  - [SH_LOOKUP_HASH_INTERNAL](SH_LOOKUP_HASH_INTERNAL.md) (when searching through collisions)
  - [SH_DELETE](SH_DELETE.md) (when probing during deletion)
  - [SH_DELETE_ITEM](SH_DELETE_ITEM.md) (when probing during item removal)

## Notes and Other Information
- Uses (curelem + 1) & tb->sizemask for efficient wraparound without expensive modulo operations
- The Assert ensures infinite loops are caught during development if the table becomes completely full
- Implements linear probing collision resolution strategy
- Part of PostgreSQL's templated simple hash table implementation where SH_PREFIX defines the specific hash table type
- Critical for hash table correctness when handling hash collisions