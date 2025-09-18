# SH_START_ITERATE

## Location
src/include/lib/simplehash.h: 983 - 1022

## Overview
A macro that expands to a hash table iteration initialization function used in PostgreSQL's simplehash system to begin iterating over all elements in a hash table.

## Definition


Function signature (after macro expansion):


## Detailed Description
SH_START_ITERATE is part of PostgreSQL's generic hash table implementation template system. When a hash table is defined using the simplehash macros, this expands to create a type-specific function that initializes an iterator for traversing all elements in the hash table. The iterator is designed to be safe for deletions during iteration by starting at an empty slot and iterating backwards. This approach allows the current element to be deleted without affecting the iteration, even if there are backward shifts in the hash table due to the deletion.

The implementation searches for the first empty element in the hash table to use as both the starting and ending point for iteration. It sets up the iterator to traverse backwards through the hash table elements, which provides deletion safety during iteration.

## Parameters / Member Variables
- : Pointer to the hash table structure to iterate over
- : Pointer to the iterator structure that will be initialized for the iteration

## Dependencies
- Functions called/Symbols referenced:
  - SH_MAKE_NAME (macro for generating type-specific names)
  - SH_STATUS_IN_USE (status constant for checking element state)
  - Assert (for debugging assertions)
- Called from (representative examples):
  - tuplehash_start_iterate (in execnodes.h)
  - filehash_start_iterate (in pg_rewind/filemap.c)
  - pgstat_entry_ref_hash_start_iterate (in pgstat_shmem.c)
  - memoize_start_iterate (in nodeMemoize.c)

## Notes and Other Information
- Part of the simplehash.h template system that generates type-specific hash table implementations
- Designed to be safe for element deletion during iteration
- Uses backward iteration strategy to handle hash table shifts during deletion
- Must be used with the corresponding SH_ITERATE macro to actually traverse the elements
- The iterator initialization finds an empty slot to ensure iteration boundaries are stable