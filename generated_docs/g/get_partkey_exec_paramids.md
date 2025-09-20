# get_partkey_exec_paramids

## Location
[src/backend/partitioning/partprune.c:3380-3415](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partprune.c#L3380-L3415)

## Overview
Loops through given pruning steps and finds out which exec Params are used, returning a Bitmapset of Param IDs.

## Definition

```c
static Bitmapset *
get_partkey_exec_paramids(List *steps)
```
## Detailed Description
This function analyzes a list of partition pruning steps to identify all execution parameters (PARAM_EXEC) that are referenced in the pruning expressions. It iterates through each PartitionPruneStepOp in the steps list and examines the expressions associated with each step. For non-constant expressions, it calls pull_exec_paramids to extract parameter IDs and accumulates them using bms_join.

This function is essential for determining which runtime parameters affect partition pruning decisions, which is crucial for query optimization and execution planning. The collected parameter IDs help the executor understand which partitions need to be re-evaluated when parameter values change.

## Parameters / Member Variables
- : List of partition pruning steps (PartitionPruneStepOp structures) to analyze

## Dependencies
- Functions called/Symbols referenced:
  - foreach (macro for list iteration)
  - lfirst (macro for list cell access)
  - IsA (macro for type checking)
  - [bms_join](../b/bms_join.md)
  - [pull_exec_paramids](../p/pull_exec_paramids.md)
  - Types: PartitionPruneStepOp, Expr, Const
- Called from:
  - [make_partitionedrel_pruneinfo](../m/make_partitionedrel_pruneinfo.md)

## Notes and Other Information
- This is a static function used within the partition pruning infrastructure
- The function optimizes by skipping constant expressions since they don't contain parameters
- The accumulated Bitmapset represents all runtime parameters that could affect pruning decisions
- Used during query planning to understand parameter dependencies in partition pruning
- Part of PostgreSQL's partition-wise optimization framework