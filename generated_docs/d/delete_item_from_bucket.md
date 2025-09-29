# delete_item_from_bucket

## Location
[src/backend/lib/dshash.c:1035-1062](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/dshash.c#L1035-L1062)

## Overview
Removes a specific hash table item from its bucket chain by searching for the exact item pointer.

## Definition
```c
static bool
delete_item_from_bucket(dshash_table *hash_table,
                        dshash_table_item *item,
                        dsa_pointer *bucket_head)
```

## Detailed Description
This function searches through a hash bucket's linked list to find and remove a specific item identified by its memory address. Unlike delete_key_from_bucket which searches by key comparison, this function performs a direct pointer comparison to locate the exact item to delete. When found, it removes the item from the chain and frees its memory.

## Parameters / Member Variables
- `hash_table`: Pointer to the dynamic shared hash table structure
- `item`: Pointer to the specific item to be deleted
- `bucket_head`: Pointer to the head of the bucket chain to search in

## Dependencies
- Functions called/Symbols referenced:
  - DsaPointerIsValid
  - [dsa_get_address](dsa_get_address.md)
  - [dsa_free](dsa_free.md)
  - [dshash_table](dshash_table.md) (type)
  - [dshash_table_item](dshash_table_item.md) (type)
  - dsa_pointer (type)
- Called from (representative examples):
  - [delete_item](delete_item.md)

## Notes and Other Information
- This is a static function, only used internally within dshash.c
- Returns true if deletion was successful, false if the item was not found in the bucket
- Uses pointer identity rather than key equality for item identification
- Efficiently handles linked list manipulation using pointer-to-pointer technique
- Properly deallocates memory by freeing the deleted item
- Useful when you have a direct reference to the item to be deleted

## Simplified Source

```c
static bool delete_item_from_bucket(dshash_table *hash_table,
                                    dshash_table_item *item,
                                    dsa_pointer *bucket_head) {
    // Walk through the bucket chain looking for the specific item
    while (DsaPointerIsValid(*bucket_head)) {
        dshash_table_item *bucket_item = dsa_get_address(hash_table->area, *bucket_head);

        // Check if this is the item we're looking for (pointer comparison)
        if (bucket_item == item) {
            // Remove item from chain and free its memory
            dsa_pointer next = item->next;
            dsa_free(hash_table->area, *bucket_head);
            *bucket_head = next;
            return true;
        }

        // Move to next item in chain
        bucket_head = &bucket_item->next;
    }

    return false; // Item not found
}
```