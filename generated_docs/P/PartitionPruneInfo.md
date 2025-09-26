# PartitionPruneInfo

## Location
src/include/nodes/plannodes.h: 1423 - 1430

## Overview
PartitionPruneInfo provides the executor with essential information needed to perform runtime partition pruning in partitioned table operations.

## Definition
```c
typedef struct PartitionPruneInfo
{
    pg_node_attr(no_equal, no_query_jumble)

    NodeTag     type;
    List       *prune_infos;
    Bitmapset  *other_subplans;
} PartitionPruneInfo;
```

## Detailed Description
PartitionPruneInfo serves as a container structure that enables the executor to perform runtime partition pruning for plan types that support arbitrary numbers of subplans, such as Append and MergeAppend nodes. The structure handles complex partitioning hierarchies that may contain multiple levels of partitioning.

The core concept revolves around mapping partitioned table indexes (as returned by partition pruning code) to subplan indexes in the execution plan. This mapping allows the executor to eliminate unnecessary subplans at runtime based on query parameters and constraints.

The structure supports multiple partitioning hierarchies within a single plan node through a List of Lists design. Each inner List represents one partitioning hierarchy, ordered so that parent partitioned tables appear before their children. The outer List accommodates multiple independent partitioning hierarchies that may exist in complex queries.

## Parameters / Member Variables
- `type`: NodeTag for type identification in PostgreSQL's node system
- `prune_infos`: List of Lists containing PartitionedRelPruneInfo nodes, where each inner List represents one run-time-prunable partition hierarchy appearing in the parent plan node's subplans
- `other_subplans`: Bitmapset containing indexes of subplans that are not covered by any PartitionedRelPruneInfo nodes and therefore must not be pruned during execution

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (node type identification)
  - List (PostgreSQL list structure)
  - Bitmapset (bit array for subplan indexing)
  - PartitionedRelPruneInfo (detailed pruning info for individual partitioned relations)
- Called from (representative examples):
  - ExecInitPartitionPruning (executor initialization for partition pruning)
  - CreatePartitionPruneState (pruning state setup)
  - create_append_plan (Append plan creation)
  - create_merge_append_plan (MergeAppend plan creation)
  - make_partition_pruneinfo (pruning info construction)

## Notes and Other Information
- The structure is designed to handle nested partitioning hierarchies where a partitioned table can itself contain partitioned child tables
- The ordering requirement (parents before children) in prune_infos ensures proper dependency resolution during runtime pruning
- other_subplans serves as a safety mechanism to preserve subplans that cannot be safely pruned
- This structure is primarily used with Append and MergeAppend plan nodes that can benefit from runtime subplan elimination
- The pg_node_attr annotations indicate this structure is not used in equality comparisons or query jumbling operations
- Runtime partition pruning can significantly improve performance by avoiding execution of subplans for partitions that cannot contain matching rows