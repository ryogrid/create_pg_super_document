# perform_pruning_base_step

## Location
src/backend/partitioning/partprune.c: 3416 - 3563

## Overview
Determines the indexes of datums that satisfy conditions specified in a partition pruning step, including whether special null-accepting and/or default partitions need to be scanned.

## Definition


## Detailed Description
This function is the core execution engine for individual partition pruning steps in PostgreSQL's partition pruning system. It takes a pruning step operation and evaluates it against partition bounds to determine which partitions might contain matching data.

The function first builds a partition lookup key by extracting values from the step's expressions, handling null values appropriately, and setting up comparison functions. It then delegates to the appropriate strategy-specific function (hash, list, or range partitioning) to perform the actual bound matching. For range partitioning, it enforces the requirement that values must be provided for either all partition keys or a prefix thereof.

The function handles cross-type comparisons by setting up appropriate comparison functions and manages function caching for performance. It returns a PruneStepResult indicating which partition bounds match the pruning criteria.

## Parameters / Member Variables
- : PartitionPruneContext containing partition metadata, bounds, and cached comparison functions
- : PartitionPruneStepOp containing the pruning operation details including expressions, comparison functions, operator strategy, and null keys

## Dependencies
- Functions called/Symbols referenced:
  - list_length, list_head, lnext (list operations)
  - [bms_is_member](../b/bms_is_member.md) (bitmap set operations)  
  - [partkey_datum_from_expr](partkey_datum_from_expr.md)
  - [fmgr_info_copy](../f/fmgr_info_copy.md), fmgr_info_cxt (function manager operations)
  - [get_matching_hash_bounds](../g/get_matching_hash_bounds.md)
  - [get_matching_list_bounds](../g/get_matching_list_bounds.md)
  - [get_matching_range_bounds](../g/get_matching_range_bounds.md)
  - PruneCxtStateIdx (macro)
  - Constants: PARTITION_MAX_KEYS, PARTITION_STRATEGY_HASH, PARTITION_STRATEGY_LIST, PARTITION_STRATEGY_RANGE
- Called from:
  - [get_matching_partitions](../g/get_matching_partitions.md)

## Notes and Other Information
- This is a static function that serves as the main entry point for executing partition pruning steps
- The function enforces strict operator semantics - null values in comparisons cause no partitions to match
- Function caching is used to optimize repeated calls with the same comparison functions
- For range partitioning, the function respects the constraint that values must form a prefix of the partition key
- The function handles all three PostgreSQL partitioning strategies (hash, list, range) through delegation
- Part of PostgreSQL's constraint exclusion and partition-wise optimization infrastructure