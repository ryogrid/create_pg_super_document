# copy_key

## Location
[src/backend/lib/dshash.c:1085-1090](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/dshash.c#L1085-L1090)

## Overview
A static inline helper function that copies a key from source to destination in the dynamic shared hash table (dshash) implementation.

## Definition

```c
static inline void
copy_key(dshash_table *hash_table, void *dest, const void *src)
```
## Detailed Description
The  function is a utility function within PostgreSQL's dynamic shared hash table implementation that handles copying keys from one location to another. It serves as a wrapper around the hash table's configured copy function, providing a consistent interface for key copying operations throughout the dshash implementation.

The function delegates the actual copying to the  stored in the hash table's parameters, passing the destination pointer, source pointer, key size, and any additional arguments. This abstraction allows different hash tables to use different key copying strategies based on their specific requirements.

## Parameters / Member Variables
- : Pointer to the dshash_table structure containing the hash table configuration and copy function
- : Pointer to the destination where the key should be copied
- : Pointer to the source key to be copied

## Dependencies
- Functions called/Symbols referenced:
  - [dshash_table](../d/dshash_table.md) (structure accessed)
  - hash_table->params.copy_function (function pointer called)
  - hash_table->params.key_size (field accessed)
  - hash_table->arg (field accessed)
- Called from (representative examples):
  - BUCKET_FOR_HASH (at src/backend/lib/dshash.c:189)
  - [insert_into_bucket](../i/insert_into_bucket.md) (at src/backend/lib/dshash.c:997)

## Notes and Other Information
- This is a static inline function, meaning it's only visible within the dshash.c file and is likely to be inlined by the compiler for performance
- The function provides flexibility for different types of keys by using a configurable copy function rather than a simple memcpy
- It's essential for operations that need to store keys in the hash table, such as insertions
- The copy function can handle complex key types that may require deep copying or special handling
- Located at src/backend/lib/dshash.c:1085-1090

## Simplified Source

```c
static inline void copy_key(dshash_table *hash_table, void *dest, const void *src) {
    // Delegate to hash table's configured copy function
    hash_table->params.copy_function(dest, src,
                                   hash_table->params.key_size,
                                   hash_table->arg);
}
```