# RelationGetPartitionQual

## Location
[src/backend/utils/cache/partcache.c:277-298](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/partcache.c#L277-L298)

## Overview
Retrieves the partition constraint qualification (WHERE clause) for a partition relation, returning the conditions that define which rows belong to this specific partition.

## Definition
```c
List *RelationGetPartitionQual(Relation rel)
```

## Detailed Description
RelationGetPartitionQual provides access to the partition constraints (quals) that define the valid row values for a specific partition. The function performs a quick check to ensure the relation is actually a partition (relispartition = true) before delegating to generate_partition_qual to construct the qualification list.

The returned list contains expressions that represent the constraints inherited from the partition hierarchy - these are the conditions that must be satisfied for a row to belong to this partition. For example, for a range partition on a date column, this might return conditions like "date_col >= '2023-01-01' AND date_col < '2023-02-01'".

## Parameters / Member Variables
- `rel`: The partition relation for which to retrieve the partition qualification

## Dependencies
- Functions called/Symbols referenced:
  - [generate_partition_qual](../g/generate_partition_qual.md) (constructs the actual partition qualification)
- Called from (representative examples):
  - [ATExecAttachPartition](../A/ATExecAttachPartition.md) (src/backend/commands/tablecmds.c:18708)
  - [DetachAddConstraintIfNeeded](../D/DetachAddConstraintIfNeeded.md) (src/backend/commands/tablecmds.c:19685)
  - [ExecPartitionCheck](../E/ExecPartitionCheck.md) (src/backend/executor/execMain.c:1814)
  - [set_baserel_partition_constraint](../s/set_baserel_partition_constraint.md) (src/backend/optimizer/util/plancat.c:2638)

## Notes and Other Information
- Returns NIL immediately if the relation is not a partition (relispartition = false)
- The returned qualification represents the partition constraints that define row membership
- Used by the executor for partition constraint checking during INSERT/UPDATE operations
- Used by the planner to establish base relation partition constraints for optimization
- Essential for partition-wise operations and constraint exclusion
- The actual constraint generation is delegated to generate_partition_qual function