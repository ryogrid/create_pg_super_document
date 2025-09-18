# SH_DELETE_ITEM

## Location
[src/include/lib/simplehash.h:928-982](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/simplehash.h#L928-L982)

## Overview
A macro that defines the public hash table deletion-by-pointer function name using the SH_MAKE_NAME naming convention for PostgreSQL's generic simple hash table implementation.

## Definition


Function signature (after macro expansion):


## Detailed Description
SH_DELETE_ITEM is a macro that expands to create a function name for the public hash table deletion operation when you already have a pointer to the entry. This is part of PostgreSQL's generic simple hash table implementation that uses C macros to generate type-specific hash table functions.

The generated function provides an optimized deletion interface when you already have a direct pointer to the entry to delete:
1. Calculates the element index from the entry pointer
2. Decrements the member count immediately
3. Performs backward shifting to maintain hash table density
4. Uses the same backward shifting algorithm as SH_DELETE but avoids the initial lookup phase

This function is more efficient than SH_DELETE when you already have the entry pointer (e.g., from a previous lookup operation) because it skips the search phase entirely.

## Parameters / Member Variables
- : Pointer to the hash table structure  
- : Direct pointer to the entry to delete

Return value: void (no return value, assumes entry is valid)

## Dependencies
- Functions called/Symbols referenced:
  - SH_MAKE_NAME (for name generation)
  - [SH_ENTRY_HASH](SH_ENTRY_HASH.md) (gets hash value from entry)
  - [SH_INITIAL_BUCKET](SH_INITIAL_BUCKET.md) (calculates starting bucket)
  - [SH_NEXT](SH_NEXT.md) (moves to next bucket in probe sequence)
- Called from (representative examples):
  - PostgreSQL subsystems that have cached entry pointers
  - Iterator-based deletion operations

## Notes and Other Information
- More efficient than SH_DELETE when you already have the entry pointer
- Uses the same backward shifting algorithm as SH_DELETE for consistency
- Assumes the entry pointer is valid (no bounds checking)
- Maintains hash table density by avoiding tombstones
- Useful for scenarios like iterator-based deletion where entries are visited sequentially
- Part of the generic simple hash table implementation that generates type-specific functions
- The function directly modifies the hash table structure and decrements member count
- No return value since it assumes the entry is valid and present