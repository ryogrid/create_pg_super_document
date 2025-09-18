# SH_START_ITERATE_AT

## Location
[src/include/lib/simplehash.h:1023-1044](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/simplehash.h#L1023-L1044)

## Overview
A macro that expands to a hash table iteration initialization function used in PostgreSQL's simplehash system to begin iterating over all elements starting from a specific position in the hash table.

## Definition
```c
#define SH_START_ITERATE_AT SH_MAKE_NAME(start_iterate_at)
```

Function signature (after macro expansion):
```c
void <prefix>_start_iterate_at(<prefix>_hash *tb, <prefix>_iterator *iter, uint32 at)
```

## Detailed Description
SH_START_ITERATE_AT is part of PostgreSQL's generic hash table implementation template system. This macro expands to create a type-specific function that initializes an iterator for traversing hash table elements starting from a specified position. Unlike SH_START_ITERATE which finds the first empty slot automatically, this function allows the caller to specify the starting position explicitly.

The iterator is designed with the same deletion-safety mechanism as SH_START_ITERATE - it uses backward iteration to allow the current element to be deleted during iteration without affecting the traversal, even if there are backward shifts in the hash table structure.

The starting position parameter 'at' is masked with the table's sizemask to ensure it falls within valid bounds of the hash table array.

## Parameters / Member Variables
- `tb`: Pointer to the hash table structure to iterate over
- `iter`: Pointer to the iterator structure that will be initialized for the iteration
- `at`: The starting position (bucket index) in the hash table where iteration should begin

## Dependencies
- Functions called/Symbols referenced:
  - SH_MAKE_NAME (macro for generating type-specific names)
  - tb->sizemask (hash table size mask for bounds checking)
- Called from (representative examples):
  - pagetable_start_iterate_at (in nodes/tidbitmap.c)

## Notes and Other Information
- Part of the simplehash.h template system that generates type-specific hash table implementations
- Provides more control than SH_START_ITERATE by allowing specification of starting position
- Uses the same deletion-safe backward iteration strategy as SH_START_ITERATE
- The 'at' parameter is automatically masked to ensure it's within valid table bounds
- Commonly used when iteration needs to resume from a specific known position
- Must be used with the corresponding SH_ITERATE macro to actually traverse the elements