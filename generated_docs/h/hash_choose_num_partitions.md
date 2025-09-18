# hash_choose_num_partitions

## Location
src/backend/executor/nodeAgg.c: 1991 - 2044

## Overview
Determines the optimal number of partitions to create when spilling hash aggregation data to disk, ensuring each partition can fit in memory while respecting system limits and hash bit constraints.

## Definition


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