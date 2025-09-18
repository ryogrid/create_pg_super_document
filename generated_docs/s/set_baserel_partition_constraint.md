# set_baserel_partition_constraint

## Location
src/backend/optimizer/util/plancat.c: 2624 - 2646

## Overview
Builds and sets the partition constraint for a base relation by retrieving the partition qualification from the relation metadata and processing it for query optimization use.

## Definition
```c
static void set_baserel_partition_constraint(Relation relation, RelOptInfo *rel)
```

## Detailed Description
This function retrieves and processes the partition constraint (qualification) for a partitioned base relation. The partition constraint represents the conditions that must be satisfied for rows to belong to this specific partition. The function performs const-simplification on the partition quals similar to check constraints but skips canonicalization since partition quals are already in canonical form.

The function includes an optimization check to avoid redundant work - if the partition_qual is already set, it returns immediately. When processing the constraint, it runs the partition quals through the expression planner for optimization and re-stamps any Var nodes with the correct relation ID to ensure proper variable references in the query plan.

The partition constraint is used by the query planner for partition pruning - eliminating partitions that cannot possibly contain matching rows based on the query conditions.

## Parameters / Member Variables
- `relation`: The Relation structure representing the partitioned table
- `rel`: The RelOptInfo structure where the partition constraint will be stored in the partition_qual field

## Dependencies
- Functions called/Symbols referenced:
  - [RelationGetPartitionQual](../R/RelationGetPartitionQual.md)
  - [expression_planner](../e/expression_planner.md)
  - [ChangeVarNodes](../C/ChangeVarNodes.md)
- Called from (representative examples):
  - [get_relation_constraints](../g/get_relation_constraints.md)
  - [set_relation_partition_info](set_relation_partition_info.md)

## Notes and Other Information
- This is a static function used internally within plancat.c for partition constraint handling
- The function includes an early return optimization to avoid reprocessing already-set partition qualifications
- Partition quals are assumed to be in canonical implicit-AND format, avoiding the need for canonicalize_qual
- The expression_planner call optimizes the partition constraint for better runtime performance
- [Variable](../V/Variable.md) node re-stamping ensures correct relation references when the relation ID is not 1
- The partition constraint is essential for partition pruning optimization during query execution