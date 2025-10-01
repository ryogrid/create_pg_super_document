# hash_choose_num_partitions

## Location
[src/backend/executor/nodeAgg.c:1991-2044](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAgg.c#L1991-L2044)

## Overview
Determines the optimal number of partitions to create when spilling hash aggregation data to disk, ensuring each partition can fit in memory while respecting system limits and hash bit constraints.

## Definition

```c
static int
hash_choose_num_partitions(double input_groups, double hashentrysize,
						   int used_bits, int *log2_npartitions)
```
## Detailed Description
This function calculates the number of partitions needed when hash aggregation must spill to disk due to memory constraints. It balances several factors: ensuring each partition will fit in available memory, limiting the memory overhead of maintaining multiple open partition files, respecting minimum and maximum partition limits, and ensuring sufficient hash bits remain available for partitioning. The result is always a power of two to enable efficient bit-based partitioning. The function also considers the memory cost of maintaining write buffers for all partitions and a read buffer.

## Parameters / Member Variables
- : The estimated number of input groups that need to be partitioned
- : The estimated size in bytes of each hash table entry
- : The number of hash bits already consumed for bucketing
- : Output parameter to store the log2 of the number of partitions (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [get_hash_memory_limit](../g/get_hash_memory_limit.md)
  - [my_log2](../m/my_log2.md)
  - HASHAGG_READ_BUFFER_SIZE
  - HASHAGG_WRITE_BUFFER_SIZE  
  - HASHAGG_PARTITION_FACTOR
  - HASHAGG_MIN_PARTITIONS
  - HASHAGG_MAX_PARTITIONS
- Called from (representative examples):
  - [hash_agg_set_limits](hash_agg_set_limits.md)
  - [hashagg_spill_init](hashagg_spill_init.md)

## Notes and Other Information
- The function ensures partition file memory overhead doesn't exceed 25% of available hash memory
- Uses HASHAGG_PARTITION_FACTOR to provide a safety margin when estimating memory needs per partition
- Enforces a minimum of HASHAGG_MIN_PARTITIONS and maximum of HASHAGG_MAX_PARTITIONS
- Partition count is constrained by remaining hash bits (total 32 bits minus used_bits)
- Returns a power-of-two value to enable efficient bit-based hash partitioning
- The algorithm prioritizes fitting partitions in memory over minimizing partition count

## Simplified Source

```c
static int hash_choose_num_partitions(double input_groups, double hashentrysize,
                                      int used_bits, int *log2_npartitions)
{
    Size hash_mem_limit = get_hash_memory_limit();
    double partition_limit;
    double mem_wanted;
    double dpartitions;
    int npartitions;
    int partition_bits;

    // Limit partitions to avoid excessive file buffer memory overhead
    // Keep partition file buffers under 25% of hash memory
    partition_limit = (hash_mem_limit * 0.25 - HASHAGG_READ_BUFFER_SIZE) /
                      HASHAGG_WRITE_BUFFER_SIZE;

    // Calculate memory needed for all input groups with safety factor
    mem_wanted = HASHAGG_PARTITION_FACTOR * input_groups * hashentrysize;

    // Choose enough partitions so each one fits in memory
    dpartitions = 1 + (mem_wanted / hash_mem_limit);

    // Apply various limits
    if (dpartitions > partition_limit)
        dpartitions = partition_limit;
    if (dpartitions < HASHAGG_MIN_PARTITIONS)
        dpartitions = HASHAGG_MIN_PARTITIONS;
    if (dpartitions > HASHAGG_MAX_PARTITIONS)
        dpartitions = HASHAGG_MAX_PARTITIONS;

    npartitions = (int) dpartitions;

    // Calculate how many bits needed (ceiling of log2)
    partition_bits = my_log2(npartitions);

    // Ensure we don't exhaust available hash bits
    if (partition_bits + used_bits >= 32)
        partition_bits = 32 - used_bits;

    // Return log2 if requested
    if (log2_npartitions != NULL)
        *log2_npartitions = partition_bits;

    // Final partition count is a power of two
    npartitions = 1 << partition_bits;

    return npartitions;
}
```