# delete_key_from_bucket

## Location
[src/backend/lib/dshash.c:1006-1034](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/dshash.c#L1006-L1034)

## Overview
Searches a hash bucket for an entry matching the given key and removes it from the bucket chain.

## Definition
```c
static bool
delete_key_from_bucket(dshash_table *hash_table,
                       const void *key,
                       dsa_pointer *bucket_head)
```

## Detailed Description
This function traverses a hash bucket's linked list to find an entry with a key that matches the provided key. If found, it removes the item from the chain by updating the previous item's next pointer (or the bucket head if it's the first item) and frees the memory associated with the deleted item. The function returns true if an item was found and deleted, false otherwise.

## Parameters / Member Variables
- `hash_table`: Pointer to the dynamic shared hash table structure
- `key`: Pointer to the key to search for and delete
- `bucket_head`: Pointer to the head of the bucket chain to search in

## Dependencies
- Functions called/Symbols referenced:
  - DsaPointerIsValid
  - [dsa_get_address](dsa_get_address.md)
  - [equal_keys](../e/equal_keys.md)
  - [dsa_free](dsa_free.md)
  - ENTRY_FROM_ITEM
  - [dshash_table](dshash_table.md) (type)
  - dsa_pointer (type)
  - [dshash_table_item](dshash_table_item.md) (type)
- Called from (representative examples):
  - [dshash_delete_key](dshash_delete_key.md)

## Notes and Other Information
- This is a static function, only used internally within dshash.c
- Returns true if deletion was successful, false if key was not found
- Properly handles memory deallocation by freeing the deleted item
- Uses pointer-to-pointer technique for efficient linked list manipulation
- The function handles deletion from any position in the linked list (head, middle, or tail)
- Only deletes the first matching entry if duplicates exist