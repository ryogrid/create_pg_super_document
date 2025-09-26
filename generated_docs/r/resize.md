# resize

## Location
[src/backend/lib/dshash.c:858-936](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/dshash.c#L858-L936)

## Overview
A static function that grows a dynamic shared hash table by doubling its bucket array size and redistributing all existing items across the new bucket layout.

## Definition
```c
static void resize(dshash_table *hash_table, size_t new_size_log2)
```

## Detailed Description
The `resize` function performs hash table expansion when the current table size becomes insufficient. It doubles the hash table size by creating a new bucket array with 2^new_size_log2 buckets and redistributing all existing items according to their hash values and the new bucket count.

The resizing process follows these key steps:
1. Acquires exclusive locks on all partitions to ensure exclusive access during the resize operation
2. Checks if another backend has already performed the resize (early exit optimization)
3. Allocates a new, larger bucket array in shared memory
4. Iterates through all existing buckets and reinserts items into the new bucket array
5. Atomically swaps the new bucket array into place and frees the old array
6. Releases all partition locks

This operation is expensive but infrequent, as hash tables only resize when they become too dense, ensuring good average-case performance.

## Parameters / Member Variables
- `hash_table`: Pointer to the dshash_table structure to be resized
- `new_size_log2`: Base-2 logarithm of the new bucket count (new size = 2^new_size_log2)

## Dependencies
- Functions called/Symbols referenced:
  - Assert
  - [LWLockHeldByMe](../L/LWLockHeldByMe.md)
  - [LWLockAcquire](../L/LWLockAcquire.md)  
  - [LWLockRelease](../L/LWLockRelease.md)
  - PARTITION_LOCK
  - [dsa_allocate_extended](../d/dsa_allocate_extended.md)
  - [dsa_get_address](../d/dsa_get_address.md)
  - [dsa_free](../d/dsa_free.md)
  - DsaPointerIsValid
  - [insert_item_into_bucket](../i/insert_item_into_bucket.md)
  - BUCKET_INDEX_FOR_HASH_AND_SIZE
- Types used:
  - [dshash_table](../d/dshash_table.md)
  - [dshash_table_item](../d/dshash_table_item.md)
  - dsa_pointer
- Constants used:
  - DSHASH_NUM_PARTITIONS
  - LW_EXCLUSIVE
  - DSA_ALLOC_HUGE
  - DSA_ALLOC_ZERO
- Called from (representative examples):
  - [dshash_find_or_insert](../d/dshash_find_or_insert.md)
  - BUCKET_FOR_HASH (indirectly)

## Notes and Other Information
- This is a static function, only accessible within the dshash.c compilation unit
- Must be called without holding any partition locks
- The new size must be exactly double the current size (new_size_log2 = current_size_log2 + 1)
- Uses exclusive locking on all partitions to prevent concurrent modifications during resize
- Includes optimization to avoid unnecessary work if another backend has already resized the table
- Memory allocation uses DSA_ALLOC_HUGE and DSA_ALLOC_ZERO flags for efficient large allocation
- The resize operation is atomic from the perspective of other backends
- All existing items are preserved and redistributed to maintain hash table correctness