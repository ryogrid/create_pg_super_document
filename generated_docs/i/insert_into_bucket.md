# insert_into_bucket

## Location
src/backend/lib/dshash.c: 986 - 1005

## Overview
Allocates space for a new hash table entry with the given key and inserts it into the specified bucket.

## Definition
```c
static dshash_table_item *
insert_into_bucket(dshash_table *hash_table,
                   const void *key,
                   dsa_pointer *bucket)
```

## Detailed Description
This function combines memory allocation and insertion operations for new hash table entries. It first allocates shared memory space for a new entry that can hold both the hash table item metadata and the user data, then copies the provided key into the allocated space, and finally links the new item into the bucket chain using insert_item_into_bucket. The function returns a pointer to the newly created and inserted item.

## Parameters / Member Variables
- `hash_table`: Pointer to the dynamic shared hash table structure
- `key`: Pointer to the key data to be copied into the new entry
- `bucket`: Pointer to the bucket where the new item should be inserted

## Dependencies
- Functions called/Symbols referenced:
  - dsa_allocate
  - [dsa_get_address](../d/dsa_get_address.md)
  - [copy_key](../c/copy_key.md)
  - [insert_item_into_bucket](insert_item_into_bucket.md)
  - ENTRY_FROM_ITEM
  - [dshash_table](../d/dshash_table.md) (type)
  - dsa_pointer (type)
  - [dshash_table_item](../d/dshash_table_item.md) (type)
- Called from (representative examples):
  - [dshash_find_or_insert](../d/dshash_find_or_insert.md)

## Notes and Other Information
- This is a static function, only used internally within dshash.c
- Combines allocation and insertion in a single operation
- The allocated size includes both the item metadata and the user entry size with proper alignment
- Uses MAXALIGN to ensure proper memory alignment for the hash table item structure
- Returns the newly inserted item for immediate use by the caller