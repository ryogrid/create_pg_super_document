# get_matching_partitions

## Location
src/backend/partitioning/partprune.c: 817 - 960

## Overview
Determines which partitions survive partition pruning by executing a list of pruning steps and returning a bitmapset of the surviving partition indexes.

## Definition


## Detailed Description
This function is the main entry point for partition pruning execution. It processes a list of pruning steps in sequence, where each step can be either a base pruning operation (PartitionPruneStepOp) or a combination operation (PartitionPruneStepCombine). The function allocates space for storing intermediate results from each pruning step, then iterates through all steps, executing them based on their type.

After all pruning steps are executed, the function collects the final result which contains bound offsets of datums whose corresponding partitions should be included. It then translates these bound offsets into actual partition indexes, handling special cases like null-accepting partitions and default partitions.

The function supports all PostgreSQL partitioning strategies (LIST, RANGE, HASH) and properly handles edge cases where bounds don't correspond to actual partitions, marking the default partition for scanning when appropriate.

## Parameters / Member Variables
- : PartitionPruneContext containing partition metadata, bound information, strategy, and execution context
- : List of PartitionPruneStep objects to be executed in sequence

## Dependencies
- Functions called/Symbols referenced:
  - bms_add_range
  - perform_pruning_base_step
  - perform_pruning_combine_step
  - bms_next_member
  - bms_add_member
  - partition_bound_has_default
  - partition_bound_accepts_nulls
  - nodeTag
- Called from (representative examples):
  - find_matching_subplans_recurse (execPartition.c:2383, 2386)
  - prune_append_rel_partitions (partprune.c:803)

## Notes and Other Information
- Returns all partitions if no pruning steps are provided
- Requires context->exprcontext to be valid when pruning_steps were generated with targets other than PARTTARGET_PLANNER
- Handles special partition types: null-accepting partitions (LIST strategy only) and default partitions (LIST/RANGE strategies)
- The function performs bounds checking and validates partition indexes before adding them to the result set
- Memory allocation for results array uses palloc0 to ensure proper initialization