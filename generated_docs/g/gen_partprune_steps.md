# gen_partprune_steps

## Location
src/backend/partitioning/partprune.c: 714 - 749

## Overview
Processes restriction clauses to generate a list of partition pruning steps that can be used for different phases of query execution.

## Definition
```c
static void gen_partprune_steps(RelOptInfo *rel, List *clauses, PartClauseTarget target, GeneratePruningStepsContext *context)
```

## Detailed Description
This function serves as the main entry point for generating partition pruning steps from a list of restriction clauses. It initializes the pruning context and handles special cases before delegating the actual step generation to gen_partprune_steps_internal.

The function handles different pruning targets:
- **Planning time**: Uses only immutable clauses for compile-time pruning
- **Executor startup**: Uses any allowable clause except those containing PARAM_EXEC parameters
- **Executor per-scan**: Uses any allowable clause including those with runtime parameters

A special optimization is applied when the partitioned table is itself a partition with a default partition. If the table shares partition keys with its parent, the parent's constraints may allow a narrower range of values, which can be useful for pruning the default partition. In this case, the table's own partition_qual is added to the clauses.

## Parameters / Member Variables
- `rel`: RelOptInfo for the partitioned relation being processed
- `clauses`: List of restriction clauses (typically from baserestrictinfo) to process for pruning
- `target`: PartClauseTarget indicating the pruning phase (planning, startup, or per-scan)
- `context`: Output parameter that receives the generated steps and subsidiary flags

## Dependencies
- Functions called/Symbols referenced:
  - [gen_partprune_steps_internal](gen_partprune_steps_internal.md)
  - partition_bound_has_default
  - [list_concat_copy](../l/list_concat_copy.md)
  - memset
- Called from (representative examples):
  - [make_partitionedrel_pruneinfo](../m/make_partitionedrel_pruneinfo.md)
  - [prune_append_rel_partitions](../p/prune_append_rel_partitions.md)

## Notes and Other Information
- Initializes all output values in the context to zero/false/NULL before processing
- Makes a copy of the clauses list when adding partition_qual to avoid modifying the original
- The actual pruning step generation logic is delegated to gen_partprune_steps_internal
- This function handles the setup and special cases while the internal function does the heavy lifting
- The partition_qual optimization is particularly important for default partition pruning in multi-level hierarchies
- The function is static and only used within the partition pruning subsystem