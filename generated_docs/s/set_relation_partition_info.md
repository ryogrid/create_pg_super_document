# set_relation_partition_info

## Location
src/backend/optimizer/util/plancat.c: 2419 - 2448

## Overview
Sets up partitioning scheme and related metadata for a partitioned table in the RelOptInfo structure during query planning.

## Definition


## Detailed Description
This static function initializes partition-related information for a partitioned table during query planning. It establishes the partition directory infrastructure if not already created, looks up the partition descriptor for the relation, and populates the RelOptInfo structure with essential partitioning metadata.

The function performs several key operations:
1. Creates a PartitionDirectory if one doesn't exist in the planner's global context
2. Looks up the partition descriptor for the relation using the partition directory
3. Finds and assigns the partition scheme using find_partition_scheme()
4. Sets boundary information and partition count from the descriptor
5. Sets up partition key expressions and constraints for the base relation

This information is crucial for partition-aware planning, including partition pruning, partition-wise joins, and other optimizations that leverage partitioning structure.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning context
- : RelOptInfo structure to populate with partition information
- : The opened partitioned relation to analyze

## Dependencies
- Functions called/Symbols referenced:
  - CreatePartitionDirectory
  - PartitionDirectoryLookup
  - find_partition_scheme
  - set_baserel_partition_key_exprs
  - set_baserel_partition_constraint
  - PartitionDesc
  - PartitionScheme
- Called from (representative examples):
  - get_relation_info

## Notes and Other Information
- This is a static function only used within plancat.c
- The function assumes the relation is already determined to be partitioned
- Creates partition directory infrastructure lazily on first use
- Essential for enabling partition-aware query planning optimizations
- The partition directory persists for the duration of the planning process to avoid repeated lookups
- All partition metadata is stored in the RelOptInfo for use throughout planning