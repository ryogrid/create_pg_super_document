# hashagg_spill_init

## Location
[src/backend/executor/nodeAgg.c:2894-2924](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAgg.c#L2894-L2924)

## Overview
Initializes the spilling infrastructure for hash aggregation by creating partitions and setting up the necessary data structures when memory limits are exceeded.

## Definition
```c
static void hashagg_spill_init(HashAggSpill *spill, LogicalTapeSet *tapeset, int used_bits, double input_groups, double hashentrysize)
```

## Detailed Description
This function is called when the hash aggregation algorithm determines that spilling to disk is necessary due to memory constraints. It initializes the spilling infrastructure by creating multiple partitions (logical tapes) to distribute hash table entries across disk storage. The function calculates the optimal number of partitions based on input parameters and sets up supporting data structures including HyperLogLog cardinality estimators for each partition.

The partitioning strategy uses hash bits to distribute tuples evenly across partitions, with each partition having its own logical tape for disk storage. The function also initializes cardinality tracking using HyperLogLog data structures to estimate the number of distinct groups in each partition, which helps with future memory planning during partition processing.

## Parameters / Member Variables
- `spill`: Pointer to HashAggSpill structure that will be initialized with partition information
- `tapeset`: LogicalTapeSet used for creating individual logical tapes for each partition  
- `used_bits`: Number of hash bits already used for hash table indexing
- `input_groups`: Estimated number of input groups for partition sizing calculations
- `hashentrysize`: Average size of hash table entries for memory planning

## Dependencies
- Functions called/Symbols referenced:
  - [hash_choose_num_partitions](hash_choose_num_partitions.md)
  - [palloc0](../p/palloc0.md)
  - LogicalTapeCreate
  - initHyperLogLog
  - HASHAGG_HLL_BIT_WIDTH
- Called from (representative examples):
  - [hash_agg_enter_spill_mode](hash_agg_enter_spill_mode.md)
  - [lookup_hash_entries](../l/lookup_hash_entries.md)
  - [agg_refill_hash_table](../a/agg_refill_hash_table.md)

## Notes and Other Information
- The function uses bit manipulation to create a hash-based partitioning scheme with shift and mask values
- Each partition gets its own HyperLogLog cardinality estimator with HASHAGG_HLL_BIT_WIDTH precision
- The number of partitions is chosen to balance between I/O efficiency and memory usage
- Partitions are implemented using PostgreSQL's logical tape abstraction for efficient disk I/O
- The spill structure is fully initialized and ready for tuple insertion after this function completes