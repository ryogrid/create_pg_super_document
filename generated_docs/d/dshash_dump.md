# dshash_dump

## Location
src/backend/lib/dshash.c: 778 - 831

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
  - LWLockHeldByMe
  - LWLockAcquire
  - LWLockRelease
  - PARTITION_LOCK
  - ensure_valid_bucket_pointers
  - BUCKET_INDEX_FOR_PARTITION
  - DsaPointerIsValid
  - dsa_get_address
  - fprintf
- Types used:
  - dshash_table
  - dshash_partition
  - dshash_table_item
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