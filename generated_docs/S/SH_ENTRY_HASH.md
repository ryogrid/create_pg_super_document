# SH_ENTRY_HASH

## Location
[src/include/lib/simplehash.h:395-411](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/simplehash.h#L395-L411)

## Overview
Retrieves or computes the hash value for a given hash table entry in PostgreSQL's simplehash implementation.

## Definition

```c
static inline uint32
SH_ENTRY_HASH(SH_TYPE * tb, SH_ELEMENT_TYPE * entry)
```
## Detailed Description
This function provides a unified interface to obtain the hash value of an entry, regardless of whether the hash is stored within the entry or needs to be computed on-demand. The behavior depends on the SH_STORE_HASH macro configuration:

- If SH_STORE_HASH is defined: Returns the pre-computed hash stored in the entry using SH_GET_HASH
- If SH_STORE_HASH is not defined: Computes the hash value by calling SH_HASH_KEY on the entry's key field

This abstraction allows the hash table implementation to optimize for either speed (pre-computed hashes) or memory usage (computed hashes) based on the user's configuration. Pre-stored hashes improve performance during operations like growing the hash table or comparing entries, while computed hashes save memory per entry.

## Parameters / Member Variables
- `*tb`: Pointer to the hash table structure
- `*entry`: Pointer to the hash table entry whose hash value is needed
## Dependencies
- Functions called/Symbols referenced:
  - SH_MAKE_NAME (macro for name generation)
  - SH_GET_HASH (conditionally, when SH_STORE_HASH is defined)
  - SH_HASH_KEY (conditionally, when SH_STORE_HASH is not defined)
  - SH_KEY (entry field containing the key when hash is computed)
- Called from (representative examples):
  - [SH_GROW](SH_GROW.md) (during hash table resizing operations)
  - [SH_INSERT_HASH_INTERNAL](SH_INSERT_HASH_INTERNAL.md) (during element insertion for Robin Hood hashing)
  - [SH_DELETE](SH_DELETE.md) and SH_DELETE_ITEM (during element deletion and shifting)
  - [SH_STAT](SH_STAT.md) (for hash table statistics and analysis)

## Notes and Other Information
- This is an internal helper function for the simplehash template system
- The macro SH_ENTRY_HASH expands to a function name with the user-defined prefix
- Critical for maintaining hash table consistency during operations that move entries
- The conditional compilation allows users to choose between time and space optimization
- When SH_STORE_HASH is used, hashes are compared before calling SH_EQUAL for better performance