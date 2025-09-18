# equal_keys

## Location
src/backend/lib/dshash.c: 1074 - 1084

## Overview
A static inline helper function that compares two keys for equality in the dynamic shared hash table (dshash) implementation.

## Definition


## Detailed Description
The  function is a utility function within PostgreSQL's dynamic shared hash table implementation that determines whether two keys are equal. It serves as a wrapper around the hash table's configured comparison function, providing a consistent interface for key equality checking throughout the dshash implementation.

The function delegates the actual comparison to the  stored in the hash table's parameters, passing the two keys, the key size, and any additional arguments. It returns true if the keys are equal (when the comparison function returns 0) and false otherwise.

## Parameters / Member Variables
- : Pointer to the dshash_table structure containing the hash table configuration and comparison function
- : Pointer to the first key to compare
- : Pointer to the second key to compare

## Dependencies
- Functions called/Symbols referenced:
  - dshash_table (structure accessed)
  - hash_table->params.compare_function (function pointer called)
  - hash_table->params.key_size (field accessed)
  - hash_table->arg (field accessed)
- Called from (representative examples):
  - BUCKET_FOR_HASH (at src/backend/lib/dshash.c:187)
  - find_in_bucket (at src/backend/lib/dshash.c:959)
  - delete_key_from_bucket (at src/backend/lib/dshash.c:1016)

## Notes and Other Information
- This is a static inline function, meaning it's only visible within the dshash.c file and is likely to be inlined by the compiler for performance
- The function assumes that the comparison function follows the standard C library convention where 0 indicates equality
- It's a critical component of the hash table's key lookup and manipulation operations
- Located at src/backend/lib/dshash.c:1074-1084