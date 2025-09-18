# dir_realloc

## Location
src/backend/utils/hash/dynahash.c: 1608 - 1646

## Overview
Reallocates and doubles the size of a hash table's directory when more segments are needed during table expansion.

## Definition


## Detailed Description
The dir_realloc function is responsible for expanding the hash table's directory structure when the current directory is full and more segments are needed. It doubles the directory size by allocating a new directory array, copying the existing segment pointers, and initializing the new entries to zero. The function includes safety checks to prevent reallocation when a maximum directory size limit is set. It uses the hash table's configured memory allocator and properly frees the old directory after successful reallocation. This is a critical function for dynamic hash table growth, enabling the table to accommodate more buckets as needed.

## Parameters / Member Variables
- : Pointer to the HTAB (hash table) structure whose directory needs reallocation

## Dependencies
- Functions called/Symbols referenced:
  - HASHSEGMENT (type and structure access)
  - NO_MAX_DSIZE
  - MemSet
  - [DynaHashAlloc](../D/DynaHashAlloc.md)
  - memcpy
  - [pfree](../p/pfree.md)
  - CurrentDynaHashCxt (global variable)
- Called from (representative examples):
  - [expand_table](../e/expand_table.md)

## Notes and Other Information
- Returns true on success, false on failure (memory allocation failure or max_dsize limit reached)
- This is a static function, only used internally within dynahash.c
- Doubles the directory size (dsize << 1) on each reallocation
- Assumes palloc-based allocation (checked with Assert)
- Sets CurrentDynaHashCxt before allocation to ensure proper memory context
- Zeros out the newly allocated directory entries to ensure clean state
- Will refuse to reallocate if max_dsize is not NO_MAX_DSIZE
- Part of the PostgreSQL dynamic hash table directory management system