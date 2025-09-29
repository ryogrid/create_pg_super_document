# find_in_bucket

## Location
[src/backend/lib/dshash.c:951-969](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/dshash.c#L951-L969)

## Overview
A static inline function that searches for a specific key within a hash table bucket chain by traversing the linked list of items and comparing keys.

## Definition
```c
static inline dshash_table_item *find_in_bucket(dshash_table *hash_table, const void *key, dsa_pointer item_pointer)
```

## Detailed Description
The `find_in_bucket` function performs a linear search through a single buckets chain of items to locate an item with a matching key. It traverses the linked list starting from the provided item_pointer, comparing each items key with the target key using the `equal_keys` function.

The function operates on a bucket that should already be locked by the caller, ensuring thread safety during the search operation. It follows the standard linked list traversal pattern, checking each item in sequence until either a matching item is found or the end of the chain is reached.

This is a fundamental building block for hash table lookup operations, providing the core search functionality within individual buckets after the hash function has determined the appropriate bucket.

## Parameters / Member Variables
- `hash_table`: Pointer to the dshash_table structure containing the hash table configuration and comparison functions
- `key`: Pointer to the key value being searched for
- `item_pointer`: DSA pointer to the first item in the bucket chain to search

## Dependencies
- Functions called/Symbols referenced:
  - DsaPointerIsValid (checks if DSA pointer is valid)
  - [dsa_get_address](../d/dsa_get_address.md) (converts DSA pointer to local memory address)
  - [equal_keys](../e/equal_keys.md) (compares two keys for equality)
  - ENTRY_FROM_ITEM (macro to extract user entry from item wrapper)
- Types used:
  - [dshash_table](../d/dshash_table.md)
  - [dshash_table_item](../d/dshash_table_item.md)
  - dsa_pointer
- Called from (representative examples):
  - [dshash_find](../d/dshash_find.md)
  - [dshash_find_or_insert](../d/dshash_find_or_insert.md)
  - BUCKET_FOR_HASH (indirectly)

## Notes and Other Information
- This is a static inline function, optimized for frequent use within the dshash.c compilation unit
- Returns a pointer to the matching item if found, or NULL if no match exists in the bucket
- The caller must ensure the bucket is properly locked before calling this function
- Performs O(n) linear search where n is the number of items in the bucket chain
- Uses the hash tables configured key comparison function through equal_keys()
- Part of the core hash table lookup mechanism, working in conjunction with the hash function and bucket addressing

## Simplified Source

```c
// Search for a key in a hash bucket chain
static inline dshash_table_item *
find_in_bucket(dshash_table *hash_table, const void *key, dsa_pointer item_pointer)
{
    // Traverse the linked list of items in this bucket
    while (DsaPointerIsValid(item_pointer)) {
        dshash_table_item *item;

        // Get the actual item from the DSA pointer
        item = dsa_get_address(hash_table->area, item_pointer);

        // Check if this item's key matches what we're looking for
        if (equal_keys(hash_table, key, ENTRY_FROM_ITEM(item))) {
            return item;  // Found it!
        }

        // Move to next item in the chain
        item_pointer = item->next;
    }

    return NULL;  // Not found in this bucket
}
```