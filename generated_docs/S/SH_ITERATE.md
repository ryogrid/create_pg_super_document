# SH_ITERATE

## Location
src/include/lib/simplehash.h: 1045 - 1071

## Overview
A macro that expands to a hash table iteration function used in PostgreSQL's simplehash system to retrieve the next element during hash table traversal.

## Definition
```c
#define SH_ITERATE SH_MAKE_NAME(iterate)
```

Function signature (after macro expansion):
```c
<element> *<prefix>_iterate(<prefix>_hash *tb, <prefix>_iterator *iter)
```

## Detailed Description
SH_ITERATE is part of PostgreSQL's generic hash table implementation template system. This macro expands to create a type-specific function that retrieves the next occupied element during hash table iteration. The function implements backward traversal through the hash table, which provides deletion safety during iteration.

The function continues iterating until it finds the next element with SH_STATUS_IN_USE status or reaches the end of the iteration (when iter->done becomes true). It moves backwards through the hash table by decrementing the current position and applying the size mask to handle wraparound. When the iterator reaches the ending position, it marks the iteration as done.

This backward iteration strategy ensures that the current element can be safely deleted during iteration without affecting the traversal, even if hash table elements are shifted due to the deletion.

## Parameters / Member Variables
- `tb`: Pointer to the hash table structure being iterated over  
- `iter`: Pointer to the iterator structure that tracks the current iteration state

## Dependencies
- Functions called/Symbols referenced:
  - SH_MAKE_NAME (macro for generating type-specific names)
  - SH_STATUS_IN_USE (status constant for checking if element is occupied)
  - tb->data (hash table data array)
  - tb->sizemask (hash table size mask for bounds checking)
- Called from (representative examples):
  - memoize_iterate (in executor/nodeMemoize.c)
  - pagetable_iterate (in nodes/tidbitmap.c)
  - tuplehash_iterate (in various executor modules)

## Notes and Other Information
- Part of the simplehash.h template system that generates type-specific hash table implementations
- Returns a pointer to the next occupied element, or NULL when iteration is complete
- Must be preceded by SH_START_ITERATE or SH_START_ITERATE_AT to initialize the iterator
- Uses backward iteration for deletion safety during traversal
- Typically used in while loops to process all elements in the hash table
- The iteration supports deletion of the current element without affecting the traversal