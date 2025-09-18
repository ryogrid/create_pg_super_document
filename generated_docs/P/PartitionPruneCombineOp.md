# PartitionPruneCombineOp

## Location
src/include/nodes/plannodes.h: 1547 - 1548

## Overview
An enumeration that specifies how to combine the results of multiple partition pruning steps when processing BoolExpr clauses during partition pruning.

## Definition


## Detailed Description
PartitionPruneCombineOp defines the logical operations used to combine partition sets from multiple pruning steps. This is essential for handling complex WHERE clauses that involve boolean expressions like AND and OR. When the partition pruner encounters a BoolExpr, it creates separate pruning steps for each argument clause, then uses a PartitionPruneStepCombine to merge the results according to the boolean logic.

UNION operations correspond to OR clauses - if any argument step indicates a partition should be scanned, it will be included. INTERSECT operations correspond to AND clauses - only partitions indicated by all argument steps will be included in the final result.

## Parameters / Member Variables
- : Combines partition sets using union (logical OR), including partitions that match any of the input conditions
- : Combines partition sets using intersection (logical AND), including only partitions that match all input conditions

## Dependencies
- Functions called/Symbols referenced: None (enum definition)
- Used by:
  - PartitionPruneStepCombine struct (as combineOp member)
  - gen_prune_step_combine() function in partprune.c
  - perform_pruning_combine_step() function in partprune.c

## Notes and Other Information
- Used specifically for BoolExpr clause processing during partition pruning optimization
- UNION operations use bms_add_members() to merge partition bitmaps, preserving all partitions from any input step  
- INTERSECT operations use bms_int_members() to find common partitions across all input steps
- The combine operations also handle null and default partition scanning decisions
- OR BoolExprs generate PARTPRUNE_COMBINE_UNION combine steps
- AND BoolExprs generate PARTPRUNE_COMBINE_INTERSECT combine steps
- Essential for partition-wise joins and complex partition elimination scenarios
- Results are cached to avoid redundant computation during query planning