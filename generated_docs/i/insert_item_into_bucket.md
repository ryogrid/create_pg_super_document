# insert_item_into_bucket

## Location
[src/backend/lib/dshash.c:970-985](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/dshash.c#L970-L985)

## Overview
Inserts an already-allocated item into a hash table bucket by linking it to the bucket's chain.

## Definition

```c
static void
insert_item_into_bucket(dshash_table *hash_table,
						dsa_pointer item_pointer,
						dshash_table_item *item,
						dsa_pointer *bucket)
```
## Detailed Description
This is a low-level utility function in PostgreSQL's dynamic shared hash table implementation that handles the actual insertion of a pre-allocated hash table item into a specific bucket. The function implements a simple linked list insertion at the head of the bucket chain, where the new item becomes the first element and points to the previously first item. This operation is used internally by the hash table resize and insertion operations.

## Parameters / Member Variables
- `*hash_table`: Pointer to the dynamic shared hash table structure
- `item_pointer`: DSA pointer to the allocated item in shared memory
- `*item`: Local address of the hash table item to insert
- `*bucket`: Pointer to the bucket (head of linked list) where the item should be inserted
## Dependencies
- Functions called/Symbols referenced:
  - [dsa_get_address](../d/dsa_get_address.md)
  - [dshash_table](../d/dshash_table.md) (type)
  - dsa_pointer (type)
  - [dshash_table_item](../d/dshash_table_item.md) (type)
- Called from (representative examples):
  - [resize](../r/resize.md)
  - [insert_into_bucket](insert_into_bucket.md)

## Notes and Other Information
- This is a static function, only used internally within dshash.c
- The function includes an assertion to verify that the item pointer and item address are consistent
- Uses head insertion strategy for O(1) insertion time
- The function assumes the item has already been allocated and initialized

## Simplified Source

```c
static void insert_item_into_bucket(dshash_table *hash_table,
                                  dsa_pointer item_pointer,
                                  dshash_table_item *item,
                                  dsa_pointer *bucket) {
    // Verify item consistency
    Assert(item == dsa_get_address(hash_table->area, item_pointer));

    // Insert at head of bucket chain
    item->next = *bucket;
    *bucket = item_pointer;
}
```