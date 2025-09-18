# dshash_partition

## Location
src/backend/lib/dshash.c: 73 - 77

## Overview
The dshash_partition struct provides tracking and synchronization information for each lock partition in a dynamic shared hash table, where each partition protects multiple buckets.

## Definition
```c
typedef struct dshash_partition
{
    LWLock      lock;       /* Protects all buckets in this partition. */
    size_t      count;      /* # of items in this partition's buckets */
} dshash_partition;
```

## Detailed Description
The dshash_partition struct implements a partitioning scheme for dynamic shared hash tables that balances concurrency with memory overhead. Initially, each partition corresponds to one bucket, but as the hash table grows, the buckets covered by each partition split, doubling the number of buckets per partition. This design allows multiple threads to operate on different partitions concurrently while maintaining data consistency within each partition through lightweight locking.

## Parameters / Member Variables
- `lock`: An LWLock that protects all buckets assigned to this partition, enabling concurrent access to different partitions
- `count`: A size_t counter tracking the total number of items across all buckets within this partition, used for load balancing and resize decisions

## Dependencies
- Functions called/Symbols referenced:
  - [LWLock](../L/LWLock.md)
- Called from (representative examples):
  - [dshash_table_control](dshash_table_control.md) (contains array of partitions)
  - dshash_create
  - [dshash_find_or_insert](dshash_find_or_insert.md)
  - dshash_dump
  - ASSERT_NO_PARTITION_LOCKS_HELD_BY_ME

## Notes and Other Information
- The partitioning scheme allows for fine-grained locking, improving concurrency over a single global lock
- Each partition initially covers one bucket, but grows to cover multiple buckets as the table expands
- The structure is designed to minimize memory overhead while providing adequate concurrency
- Cache line alignment was considered but not implemented to avoid bloating the structure
- The count field enables efficient load monitoring and helps determine when table resizing is needed