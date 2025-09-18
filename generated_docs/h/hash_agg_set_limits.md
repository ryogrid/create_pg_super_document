# hash_agg_set_limits

## Location
[src/backend/executor/nodeAgg.c:1798-1855](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAgg.c#L1798-L1855)

## Overview
Calculates and sets memory and group count limits for hash aggregation operations to prevent exceeding the hash_mem work_mem limit, considering the expected number of partitions required for spilling.

## Definition
```c
void hash_agg_set_limits(double hashentrysize, double input_groups, int used_bits,
                        Size *mem_limit, uint64 *ngroups_limit,
                        int *num_partitions)
```

## Detailed Description
This function establishes operational limits for hash aggregation to ensure memory usage stays within configured bounds. It employs a two-tier limiting strategy:

1. **Memory Limit**: Controls the total memory consumption of the hash table
2. **Groups Limit**: Controls the number of distinct groups that can be processed simultaneously

The function considers whether spilling is expected based on input size estimates. If spilling is not expected, it uses the full hash_mem limit. If spilling is expected, it:
- Calculates the number of partitions needed using hash_choose_num_partitions()
- Reserves memory for tape buffers (read/write operations during spilling)
- Adjusts the hash table memory limit accordingly
- Ensures the limit doesn't drop below 75% of hash_mem as a safety measure

## Parameters / Member Variables
- `hashentrysize`: Estimated size of each hash table entry in bytes
- `input_groups`: Expected number of distinct groups in the input data  
- `used_bits`: Number of bits already used for partitioning (affects partition calculation)
- `mem_limit`: Output parameter - maximum memory the hash table can use
- `ngroups_limit`: Output parameter - maximum number of groups that can fit in memory
- `num_partitions`: Output parameter - number of partitions that will be created (optional, can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [get_hash_memory_limit](../g/get_hash_memory_limit.md)
  - [hash_choose_num_partitions](hash_choose_num_partitions.md)
  - HASHAGG_READ_BUFFER_SIZE
  - HASHAGG_WRITE_BUFFER_SIZE
- Called from (representative examples):
  - [agg_refill_hash_table](../a/agg_refill_hash_table.md)
  - [ExecInitAgg](../E/ExecInitAgg.md)
  - [cost_agg](../c/cost_agg.md)

## Notes and Other Information
- The function implements a safety margin by ensuring limits don't drop below 75% of hash_mem when at minimum partitions
- Both memory and group limits are important because transition values may grow substantially beyond their initial size
- Buffer memory reservation accounts for one read buffer and one write buffer per partition for spill operations
- The group limit calculation ensures at least one group can always be processed to prevent degenerate cases