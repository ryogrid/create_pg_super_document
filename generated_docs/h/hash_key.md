# hash_key

## Location
src/backend/lib/dshash.c: 1063 - 1073

## Overview
Computes the hash value for a given key using the hash table's configured hash function.

## Definition
```c
static inline dshash_hash
hash_key(dshash_table *hash_table, const void *key)
```

## Detailed Description
This is a simple wrapper function that invokes the user-defined hash function stored in the hash table's parameters. It passes the key, key size, and optional argument to the hash function and returns the computed hash value. The function is declared as inline for optimal performance since it's called frequently during hash table operations.

## Parameters / Member Variables
- `hash_table`: Pointer to the dynamic shared hash table structure containing the hash function parameters
- `key`: Pointer to the key data to be hashed

## Dependencies
- Functions called/Symbols referenced:
  - hash_table->params.hash_function (user-defined hash function)
  - dshash_table (type)
  - dshash_hash (return type)
- Called from (representative examples):
  - dshash_find
  - dshash_find_or_insert
  - dshash_delete_key
  - compute_tsvector_stats

## Notes and Other Information
- This is a static inline function for maximum performance
- Acts as an abstraction layer between the hash table implementation and user-defined hash functions
- The hash function, key size, and optional argument are all configured when the hash table is created
- Used by all major hash table operations that need to determine bucket placement
- The actual hash computation is delegated to the user-provided hash function
- Returns a dshash_hash value which is typically used with modulo operations to determine the target bucket