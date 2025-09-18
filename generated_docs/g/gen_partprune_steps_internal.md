# gen_partprune_steps_internal

## Location
src/backend/partitioning/partprune.c: 961 - 1312

## Overview
Processes a list of WHERE clauses to generate partition pruning steps that can be used to eliminate partitions during query execution, returning NIL when no pruning steps can be generated.

## Definition


## Detailed Description
This is the core function for generating partition pruning steps from SQL WHERE clauses. It recursively processes various clause types (BoolExpr, OpExpr, IS NULL/NOT NULL) and creates two types of pruning steps:

1. **Operator Steps (PartitionPruneStepOp)**: Contain details of expressions being compared to partition keys with comparison functions
2. **Combine Steps (PartitionPruneStepCombine)**: Instructions for merging results from multiple steps using UNION (OR logic) or INTERSECT (AND logic)

The function handles three main pruning strategies:
- **Strategy 1**: IS NULL clauses - generates steps to select only null-accepting partitions
- **Strategy 2**: OpExpr-based pruning using partition key comparisons
- **Strategy 3**: IS NOT NULL clauses - generates steps to exclude null-accepting partitions

For BoolExpr clauses, it processes arguments recursively: OR expressions use UNION combine steps, AND expressions use INTERSECT combine steps. The function detects contradictory clauses and optimization opportunities, setting context->contradictory when pruning can eliminate all partitions.

## Parameters / Member Variables
- : GeneratePruningStepsContext containing partition metadata, relation info, and step generation state
- : List of WHERE clause expressions to process for partition pruning

## Dependencies
- Functions called/Symbols referenced:
  - partition_bound_has_default
  - predicate_refuted_by
  - is_orclause
  - is_andclause
  - match_clause_to_partition_key
  - gen_prune_step_op
  - gen_prune_step_combine
  - gen_prune_steps_from_opexps
  - bms_is_member
  - bms_add_member
  - bms_is_empty
  - bms_num_members
  - list_concat
  - lappend_int
  - llast
- Called from (representative examples):
  - gen_partprune_steps (partprune.c:736)
  - gen_partprune_steps_internal (recursive calls at lines 1043, 1114)
  - match_clause_to_partition_key (partprune.c:1855, 2368)

## Notes and Other Information
- Returns NIL when contradictory clauses are found or no pruning is possible
- Supports all PostgreSQL partitioning strategies: LIST, RANGE, and HASH
- Handles special cases like default partitions and null-accepting partitions
- Automatically combines multiple steps with INTERSECT when clauses are mutually ANDed
- Uses keyclauses array indexed by partition key position to organize clause matching
- Maintains nullkeys and notnullkeys bitmapsets to track IS NULL/NOT NULL constraints
- The function is recursive for handling nested BoolExpr structures
- Memory management relies on the current memory context for temporary allocations