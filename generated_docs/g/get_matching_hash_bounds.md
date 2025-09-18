# get_matching_hash_bounds

## Location
src/backend/partitioning/partprune.c: 2663 - 2739

## Overview
Determines which hash partition bound matches the specified values by computing the hash value and finding the corresponding partition offset.

## Definition


## Detailed Description
This function implements hash partition pruning by calculating the hash value for the given partition key values and determining which specific hash partition should be accessed. For hash partitioning, pruning can only be performed when:

1. All partition keys have either equality clauses or IS NULL clauses
2. The operator strategy is hash equality (HTEqualStrategyNumber)

When all keys are provided, the function computes the partition hash using the supplied values and null indicators, then uses modulo arithmetic to determine the target partition index. If not all keys are provided, it conservatively returns all partition offsets since hash pruning requires complete key information. The function handles the absence of special null or default partitions in hash partitioning.

## Parameters / Member Variables
- : Partition pruning context containing boundary info and partitioning metadata
- : Strategy number, must be HTEqualStrategyNumber for hash equality or zero
- : Array of Datum values indexed by partition key position
- : Number of values in the values array
- : Array of partition hashing functions for each partition key type
- : Bitmapset indicating which partition keys are NULL

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md)
  - [bms_num_members](../b/bms_num_members.md)
  - [bms_is_member](../b/bms_is_member.md)
  - [compute_partition_hash_value](../c/compute_partition_hash_value.md)
  - [bms_make_singleton](../b/bms_make_singleton.md)
  - [bms_add_range](../b/bms_add_range.md)
- Called from:
  - [perform_pruning_base_step](../p/perform_pruning_base_step.md)

## Notes and Other Information
Hash partitioning requires all partition key values to perform effective pruning - partial key information results in scanning all partitions. The function uses the greatest_modulus (total number of partition indexes) to compute the final partition offset. Unlike range and list partitioning, hash partitioning does not support special null or default partitions, so scan_null and scan_default are always set to false. The hash computation considers both explicit values and NULL indicators to ensure consistent partition assignment.