# PartitionPruneStepCombine

## Location
[src/include/nodes/plannodes.h:1549-1555](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/plannodes.h#L1549-L1555)

## Overview
PartitionPruneStepCombine is a concrete implementation of PartitionPruneStep that combines partition sets from multiple pruning steps using Boolean operations, handling BoolExpr clauses in partition pruning logic.

## Definition
```c
typedef struct PartitionPruneStepCombine
{
    PartitionPruneStep step;

    PartitionPruneCombineOp combineOp;
    List       *source_stepids;
} PartitionPruneStepCombine;
```

## Detailed Description
PartitionPruneStepCombine extends PartitionPruneStep to handle complex Boolean expressions in partition pruning. When the query planner encounters BoolExpr clauses (such as AND/OR operations), this structure combines the results from multiple individual pruning steps to determine the final set of partitions to scan.

The structure works by taking the results from source pruning steps (referenced by their step IDs) and combining them using either UNION or INTERSECT operations. For AND clauses, it intersects the partition sets (only partitions present in all source steps are included). For OR clauses, it unions the partition sets (partitions present in any source step are included).

This enables PostgreSQL to handle complex WHERE clauses with multiple conditions efficiently by breaking them down into individual pruning operations and then combining the results.

## Parameters / Member Variables
- `step`: Base PartitionPruneStep structure containing type and step_id
- `combineOp`: The combination operation to apply - either PARTPRUNE_COMBINE_UNION (for OR operations) or PARTPRUNE_COMBINE_INTERSECT (for AND operations)  
- `source_stepids`: List of integers representing the step IDs of the source pruning steps whose results should be combined

## Dependencies
- Functions called/Symbols referenced:
  - [PartitionPruneStep](PartitionPruneStep.md) (base structure)
  - [PartitionPruneCombineOp](PartitionPruneCombineOp.md) (enum for combine operations)
  - [List](../L/List.md) (PostgreSQL list structure)

- Called from (representative examples):
  - [get_matching_partitions](../g/get_matching_partitions.md) (src/backend/partitioning/partprune.c:858)
  - [gen_prune_step_combine](../g/gen_prune_step_combine.md) (src/backend/partitioning/partprune.c:1350)
  - [perform_pruning_combine_step](../p/perform_pruning_combine_step.md) (src/backend/partitioning/partprune.c:3565)

## Notes and Other Information
- Essential for handling complex Boolean expressions in partition pruning
- Works in conjunction with PartitionPruneStepOp to build complete pruning logic
- The source_stepids reference other steps within the same pruning context
- PARTPRUNE_COMBINE_UNION corresponds to OR logic (include partitions from any source)
- PARTPRUNE_COMBINE_INTERSECT corresponds to AND logic (include only partitions common to all sources)
- Part of PostgreSQL advanced partition pruning system for optimizing complex queries