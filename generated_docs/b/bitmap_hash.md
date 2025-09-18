# bitmap_hash

## Location
[src/backend/nodes/bitmapset.c:1432-1441](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/bitmapset.c#L1432-L1441)

## Overview
The `bitmap_hash` function serves as a hash table hash function for keys that are pointers to Bitmapsets, following PostgreSQL's standard hash function interface.

## Definition
```c
uint32 bitmap_hash(const void *key, Size keysize)
```

## Detailed Description
This function acts as a wrapper around `bms_hash_value` to provide a hash function that conforms to PostgreSQL's standard hash table interface. It's specifically designed to hash keys that are pointers to Bitmapsets (i.e., `Bitmapset *` values stored as hash table keys).

The function performs pointer dereferencing to extract the actual Bitmapset from the key pointer, then delegates the hash computation to `bms_hash_value`. It includes an assertion to verify that the key size matches the expected size of a Bitmapset pointer.

This function is intended for use with PostgreSQL's hash table infrastructure, where hash functions must follow a specific signature pattern that takes a generic pointer and key size.

## Parameters
- `key`: Pointer to the key data (expected to be a `Bitmapset **`)
- `keysize`: Size of the key data (must equal `sizeof(Bitmapset *)`)

## Dependencies
- Functions called/Symbols referenced:
  - [bms_hash_value](bms_hash_value.md)
- Called from (examples):
  - [build_join_rel_hash](build_join_rel_hash.md)
  - bms_is_empty (header usage)

## Notes and Other Information
- Must be paired with `bitmap_match` as the comparison function in hash tables
- The function expects the key to be a pointer to a Bitmapset pointer (double indirection)
- Includes runtime assertion to verify correct key size usage
- Part of the standard interface for using Bitmapsets as hash table keys
- Enables efficient hash-based lookups and storage of Bitmapset-keyed data structures