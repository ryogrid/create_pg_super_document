# dshash_table_control

## Location
src/backend/lib/dshash.c: 83 - 98

## Overview
The dshash_table_control struct serves as the main control structure for a dynamic shared hash table, stored in dynamic shared memory and containing all metadata necessary for hash table management.

## Definition
```c
typedef struct dshash_table_control
{
    dshash_table_handle handle;
    uint32              magic;
    dshash_partition    partitions[DSHASH_NUM_PARTITIONS];
    int                 lwlock_tranche_id;

    /*
     * The following members are written to only when ALL partitions locks are
     * held.  They can be read when any one partition lock is held.
     */

    /* Number of buckets expressed as power of 2 (8 = 256 buckets). */
    size_t              size_log2;      /* log2(number of buckets) */
    dsa_pointer         buckets;        /* current bucket array */
} dshash_table_control;
```

## Detailed Description
The dshash_table_control struct is the central control structure for PostgreSQL's dynamic shared hash table implementation. It resides in dynamic shared memory and coordinates all hash table operations across multiple processes. The structure contains partitioning information for concurrent access, table sizing metadata, and pointers to the actual bucket storage. The design employs a careful locking protocol where certain fields require all partition locks to be held for writes but only a single partition lock for reads, enabling efficient concurrent operations while maintaining consistency during structural changes like table resizing.

## Parameters / Member Variables
- `handle`: A dshash_table_handle that uniquely identifies this hash table instance within the dynamic shared area
- `magic`: A uint32 magic number used for validation and corruption detection of the control structure
- `partitions`: An array of dshash_partition structures (size DSHASH_NUM_PARTITIONS) that manage locking and item counting for different table regions
- `lwlock_tranche_id`: An integer identifier for the lightweight lock tranche used by this hash table's partitions
- `size_log2`: A size_t value representing the logarithm base 2 of the current number of buckets (e.g., 8 means 256 buckets)
- `buckets`: A dsa_pointer pointing to the current bucket array in dynamic shared memory

## Dependencies
- Functions called/Symbols referenced:
  - dshash_table_handle
  - dshash_partition
  - DSHASH_NUM_PARTITIONS
  - dsa_pointer
- Called from (representative examples):
  - dshash_table (contains pointer to control structure)
  - dshash_create

## Notes and Other Information
- The structure is designed for multi-process shared access through dynamic shared memory
- The locking protocol ensures that size_log2 and buckets can be read with any single partition lock but require all partition locks for modification
- The magic field provides corruption detection in shared memory environments
- The size_log2 field allows efficient bucket calculation using bit operations instead of modulo arithmetic
- The partitions array enables fine-grained concurrency control across different regions of the hash table
- This is the primary coordination structure that enables the hash table to be shared across PostgreSQL processes