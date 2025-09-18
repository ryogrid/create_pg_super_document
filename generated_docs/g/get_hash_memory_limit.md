# get_hash_memory_limit

## Location
src/backend/executor/nodeHash.c: 3602 - 3613

## Overview
Calculates the maximum memory limit available for hash-based operations by multiplying work_mem with the hash_mem_multiplier configuration parameter.

## Definition
```c
size_t
get_hash_memory_limit(void)
```

## Detailed Description
This function provides a centralized calculation of memory limits for hash-based executor nodes and planner operations. It computes the limit by multiplying the work_mem configuration parameter (measured in kilobytes) by the hash_mem_multiplier setting, then converting to bytes.

The calculation is performed in floating-point arithmetic to handle potential overflow during multiplication, then clamped to ensure it fits within the size_t data type range. This prevents integer overflow issues when dealing with large memory configurations.

The function is designed to be used by:
- Hash join operations for determining optimal hash table sizes
- Hash aggregation for partitioning decisions
- Query planner for cost estimation
- Other hash-based operations like memoization

This centralized approach ensures consistent memory limit calculations across all hash operations in PostgreSQL.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - work_mem (global variable)
  - hash_mem_multiplier (global variable) 
  - Min (macro)
  - SIZE_MAX (constant)
- Called from:
  - BuildTupleHashTableExt
  - [hash_agg_set_limits](../h/hash_agg_set_limits.md)
  - [hash_choose_num_partitions](../h/hash_choose_num_partitions.md)
  - ExecChooseHashTableSize
  - [ExecParallelHashIncreaseNumBatches](../E/ExecParallelHashIncreaseNumBatches.md)
  - [ExecInitMemoize](../E/ExecInitMemoize.md)
  - [cost_memoize_rescan](../c/cost_memoize_rescan.md)
  - [final_cost_hashjoin](../f/final_cost_hashjoin.md)
  - [consider_groupingsets_paths](../c/consider_groupingsets_paths.md)
  - [subplan_is_hashable](../s/subplan_is_hashable.md)
  - [subpath_is_hashable](../s/subpath_is_hashable.md)
  - [choose_hashed_setop](../c/choose_hashed_setop.md)
  - [create_unique_path](../c/create_unique_path.md)

## Notes and Other Information
- The function performs overflow protection by using double arithmetic and clamping to SIZE_MAX
- Exported for use by both executor nodes and the query planner
- work_mem is in KB, so multiplication by 1024.0 converts to bytes
- The hash_mem_multiplier allows PostgreSQL to allocate more memory for hash operations than the base work_mem setting
- Located in src/backend/executor/nodeHash.c:3602-3613
- According to comments, this location is "rather random" but chosen for lack of a better place