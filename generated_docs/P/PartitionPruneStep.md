# PartitionPruneStep

## Location
[src/include/nodes/plannodes.h:1492-1498](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/plannodes.h#L1492-L1498)

## Overview
PartitionPruneStep is an abstract base struct for partition pruning steps in PostgreSQL's partitioning system, serving as the foundation for concrete pruning step types like PartitionPruneStepOp and PartitionPruneStepCombine.

## Definition

```c
typedef struct PartitionPruneStep
{
	pg_node_attr(abstract, no_equal, no_query_jumble)

	NodeTag		type;
	int			step_id;
} PartitionPruneStep;
```
## Detailed Description
PartitionPruneStep serves as an abstract Node type within PostgreSQL's partition pruning framework. It provides the common base structure for all types of partition pruning steps. The struct is marked as abstract, meaning there are no concrete instances of this exact type - it exists purely as a base for inheritance by more specific pruning step types. This design allows the partition pruning system to handle different types of pruning operations uniformly while maintaining type safety through the NodeTag system.

The partition pruning system uses these steps to build a logical representation of how partitions should be eliminated during query planning, enabling PostgreSQL to avoid scanning irrelevant partitions and improve query performance.

## Parameters / Member Variables
- : NodeTag identifying the specific concrete type of partition pruning step (since this is an abstract base)
- : Global identifier for this step within its pruning context, used to reference and coordinate between different steps

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (PostgreSQL's node type system)
  
- Called from (representative examples):
  - get_matching_partitions (src/backend/partitioning/partprune.c:845)
  - gen_partprune_steps_internal (src/backend/partitioning/partprune.c:1066)
  - gen_prune_step_op (src/backend/partitioning/partprune.c:1335)
  - gen_prune_step_combine (src/backend/partitioning/partprune.c:1358)

## Notes and Other Information
- This is marked as an abstract type with pg_node_attr(abstract, no_equal, no_query_jumble), indicating it should not be instantiated directly
- The concrete implementations are PartitionPruneStepOp and PartitionPruneStepCombine
- Part of PostgreSQL's partition pruning optimization system introduced to improve performance with partitioned tables
- The step_id allows for building complex pruning logic by referencing steps from other steps