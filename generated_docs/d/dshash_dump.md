# dshash_dump

## Location
[src/backend/lib/dshash.c:778-831](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/dshash.c#L778-L831)

## Overview
A debugging function that prints detailed internal state information about a dynamic shared hash table to stderr, including partition counts and bucket chain lengths.

## Definition
```c
void dshash_dump(dshash_table *hash_table)
```

## Detailed Description
The `dshash_dump` function provides comprehensive debugging output for dynamic shared hash tables by traversing all partitions and buckets to display their current state. The function acquires shared locks on all partitions to ensure a consistent snapshot of the hash table state during inspection.

The function outputs:
- Overall hash table size (total number of buckets)
- For each partition: the partition number, total key count, and individual bucket statistics
- For each bucket: the bucket index and the number of items in its chain

This debugging capability is essential for performance analysis, understanding hash distribution, and diagnosing issues with bucket balancing or excessive chain lengths that could impact lookup performance.

## Parameters / Member Variables
- `hash_table`: Pointer to the dshash_table structure representing the dynamic shared hash table to be dumped

## Dependencies
- Functions called/Symbols referenced:
  - Assert
  - ASSERT_NO_PARTITION_LOCKS_HELD_BY_ME
  - [LWLockHeldByMe](../L/LWLockHeldByMe.md)
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [LWLockRelease](../L/LWLockRelease.md)
  - PARTITION_LOCK
  - [ensure_valid_bucket_pointers](../e/ensure_valid_bucket_pointers.md)
  - BUCKET_INDEX_FOR_PARTITION
  - DsaPointerIsValid
  - [dsa_get_address](dsa_get_address.md)
  - fprintf
- Types used:
  - [dshash_table](dshash_table.md)
  - [dshash_partition](dshash_partition.md)
  - [dshash_table_item](dshash_table_item.md)
  - dsa_pointer
- Constants used:
  - DSHASH_MAGIC
  - DSHASH_NUM_PARTITIONS
  - LW_SHARED
- Called from (representative examples):
  - No references found (debugging function typically called manually)

## Notes and Other Information
- The caller must hold no partition locks before calling this function
- The function temporarily acquires shared locks on all partitions to ensure consistency
- Output is written to stderr for debugging purposes
- This is primarily a development and debugging tool, not intended for production use
- The function validates the hash table magic number to ensure data structure integrity
- Bucket traversal follows the linked list chain structure of each bucket

## Simplified Source

```c
void dshash_dump(dshash_table *hash_table) {
    // Validate hash table and ensure no locks held
    Assert(hash_table->control->magic == DSHASH_MAGIC);
    ASSERT_NO_PARTITION_LOCKS_HELD_BY_ME(hash_table);

    // Acquire shared locks on all partitions for consistent snapshot
    for (int i = 0; i < DSHASH_NUM_PARTITIONS; ++i) {
        LWLockAcquire(PARTITION_LOCK(hash_table, i), LW_SHARED);
    }

    ensure_valid_bucket_pointers(hash_table);

    // Print overall hash table size
    fprintf(stderr, "hash table size = %zu\n", (size_t) 1 << hash_table->size_log2);

    // Iterate through each partition and its buckets
    for (int i = 0; i < DSHASH_NUM_PARTITIONS; ++i) {
        dshash_partition *partition = &hash_table->control->partitions[i];
        size_t begin = BUCKET_INDEX_FOR_PARTITION(i, hash_table->size_log2);
        size_t end = BUCKET_INDEX_FOR_PARTITION(i + 1, hash_table->size_log2);

        fprintf(stderr, "  partition %zu\n", i);
        fprintf(stderr, "    active buckets (key count = %zu)\n", partition->count);

        // Count items in each bucket chain
        for (size_t j = begin; j < end; ++j) {
            size_t count = 0;
            dsa_pointer bucket = hash_table->buckets[j];

            // Traverse linked list of items in bucket
            while (DsaPointerIsValid(bucket)) {
                dshash_table_item *item = dsa_get_address(hash_table->area, bucket);
                bucket = item->next;
                ++count;
            }
            fprintf(stderr, "      bucket %zu (key count = %zu)\n", j, count);
        }
    }

    // Release all partition locks
    for (int i = 0; i < DSHASH_NUM_PARTITIONS; ++i) {
        LWLockRelease(PARTITION_LOCK(hash_table, i));
    }
}
```