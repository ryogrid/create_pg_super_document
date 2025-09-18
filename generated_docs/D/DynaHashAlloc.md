# DynaHashAlloc

## Location
src/backend/utils/hash/dynahash.c: 291 - 306

## Overview
DynaHashAlloc is a static memory allocation function that provides memory allocation specifically for dynamic hash table operations within PostgreSQL's hash table infrastructure.

## Definition


## Detailed Description
DynaHashAlloc is a specialized memory allocation function that allocates memory from the CurrentDynaHashCxt memory context. This function serves as the standard allocator for dynamic hash table operations, ensuring that hash table memory is managed within the appropriate memory context. The function uses MCXT_ALLOC_NO_OOM flag to indicate that it should not throw an error on out-of-memory conditions, instead returning NULL to allow the caller to handle allocation failures gracefully.

## Parameters / Member Variables
- : The number of bytes to allocate for the hash table operation

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextIsValid
  - MemoryContextAllocExtended  
  - MCXT_ALLOC_NO_OOM
- Called from (representative examples):
  - hash_create
  - hash_destroy
  - dir_realloc
  - MOD

## Notes and Other Information
- This is a static function, meaning it's only accessible within the dynahash.c file
- Uses CurrentDynaHashCxt as the memory context for all allocations
- Returns NULL on allocation failure rather than throwing an error
- Essential for the internal memory management of PostgreSQL's dynamic hash table implementation
- Located at src/backend/utils/hash/dynahash.c:291-306