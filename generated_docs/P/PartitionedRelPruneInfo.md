# PartitionedRelPruneInfo

## Location
src/include/nodes/plannodes.h: 1449 - 1484

## Overview
PartitionedRelPruneInfo contains detailed mapping and pruning information required by the executor to efficiently prune partitions for a single partitioned table within a partitioning hierarchy.

## Definition
```c
typedef struct PartitionedRelPruneInfo
{
    pg_node_attr(no_equal, no_query_jumble)

    NodeTag     type;

    /* RT index of partition rel for this level */
    Index       rtindex;

    /* Indexes of all partitions which subplans or subparts are present for */
    Bitmapset  *present_parts;

    /* Length of the following arrays: */
    int         nparts;

    /* subplan index by partition index, or -1 */
    int        *subplan_map pg_node_attr(array_size(nparts));

    /* subpart index by partition index, or -1 */
    int        *subpart_map pg_node_attr(array_size(nparts));

    /* relation OID by partition index, or 0 */
    Oid        *relid_map pg_node_attr(array_size(nparts));

    /*
     * initial_pruning_steps shows how to prune during executor startup (i.e.,
     * without use of any PARAM_EXEC Params); it is NIL if no startup pruning
     * is required.  exec_pruning_steps shows how to prune with PARAM_EXEC
     * Params; it is NIL if no per-scan pruning is required.
     */
    List       *initial_pruning_steps;  /* List of PartitionPruneStep */
    List       *exec_pruning_steps;     /* List of PartitionPruneStep */

    /* All PARAM_EXEC Param IDs in exec_pruning_steps */
    Bitmapset  *execparamids;
} PartitionedRelPruneInfo;
```

## Detailed Description
PartitionedRelPruneInfo provides comprehensive mapping information that allows the executor to translate between partition indexes (as defined in the table's PartitionDesc) and execution plan structures. This structure is essential for runtime partition pruning in hierarchical partitioning scenarios.

The structure maintains three key mapping arrays indexed by partition index: subplan_map for leaf partitions that map to actual subplans, subpart_map for non-leaf partitions that reference deeper levels in the hierarchy, and relid_map for partition OID storage. These maps enable efficient translation between logical partition organization and physical execution plan structure.

The pruning is performed at two distinct phases: initial pruning during executor startup using static conditions, and runtime pruning using PARAM_EXEC parameters that may change between scan iterations. This dual-phase approach optimizes both startup time and per-scan execution costs.

## Parameters / Member Variables
- `type`: NodeTag for type identification in PostgreSQL's node system
- `rtindex`: Range table index of the partitioned relation for this hierarchy level
- `present_parts`: Bitmapset indicating which partitions have corresponding subplans or sub-partitions present
- `nparts`: Number of partitions at this level, determining the length of the mapping arrays
- `subplan_map`: Array mapping partition indexes to zero-based subplan indexes in the parent plan's subplan list (-1 for non-leaf or pruned partitions)
- `subpart_map`: Array mapping partition indexes to zero-based indexes in the hierarchy's PartitionedRelPruneInfo list (-1 for leaf or pruned partitions)
- `relid_map`: Array mapping partition indexes to partition OIDs (0 for pruned partitions)
- `initial_pruning_steps`: List of PartitionPruneStep nodes for startup-time pruning without PARAM_EXEC parameters
- `exec_pruning_steps`: List of PartitionPruneStep nodes for runtime pruning using PARAM_EXEC parameters
- `execparamids`: Bitmapset containing all PARAM_EXEC parameter IDs referenced in exec_pruning_steps

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (node type identification)
  - Index (range table and mapping indexes)
  - Bitmapset (efficient bit array operations)
  - Oid (object identifier storage)
  - List (PostgreSQL list structure)
  - PartitionPruneStep (individual pruning step definitions)
- Called from (representative examples):
  - CreatePartitionPruneState (executor pruning state creation)
  - make_partitionedrel_pruneinfo (pruning info construction)
  - set_append_references (Append plan reference resolution)
  - set_mergeappend_references (MergeAppend plan reference resolution)

## Notes and Other Information
- Subplan indexes stored in subplan_map are global across the entire parent plan node, while partition indexes are local to each hierarchy level
- The distinction between leaf and non-leaf partitions is crucial: leaf partitions map to actual subplans, non-leaf partitions map to deeper PartitionedRelPruneInfo entries
- The dual pruning approach (initial vs exec) allows optimization of both query startup time and per-iteration costs in prepared statements
- Pruned partitions are represented by -1 in mapping arrays and 0 in relid_map, providing a clear indication of eliminated partitions
- The structure supports complex nested partitioning hierarchies where a partitioned table can contain other partitioned tables as children
- PARAM_EXEC parameters enable dynamic partition pruning based on runtime values, significantly improving performance for prepared statements and parameterized queries