# PartitionPruneContext

## Location
[src/include/partitioning/partprune.h:49-62](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/partitioning/partprune.h#L49-L62)

## Overview
PartitionPruneContext is a PostgreSQL data structure that stores information needed at runtime for pruning computations related to a single partitioned table, enabling the elimination of irrelevant partitions during query execution.

## Definition
```c
typedef struct PartitionPruneContext
{
    char            strategy;
    int             partnatts;
    int             nparts;
    PartitionBoundInfo boundinfo;
    Oid            *partcollation;
    FmgrInfo       *partsupfunc;
    FmgrInfo       *stepcmpfuncs;
    MemoryContext   ppccontext;
    PlanState      *planstate;
    ExprContext    *exprcontext;
    ExprState     **exprstates;
} PartitionPruneContext;
```
*Location: src/include/partitioning/partprune.h:49-62*

## Detailed Description
PartitionPruneContext serves as the central data structure for PostgreSQL's partition pruning mechanism. It encapsulates all necessary runtime information to efficiently determine which partitions can be eliminated during query execution based on the query's WHERE clause conditions. The structure supports all PostgreSQL partitioning strategies (LIST, RANGE, HASH) and provides the infrastructure for both planner-time and execution-time pruning decisions.

The context maintains partition metadata, comparison functions, memory management, and execution state to enable efficient partition elimination. It bridges the gap between the static partition definition and the dynamic pruning process by providing access to partition boundaries, comparison functions, and expression evaluation capabilities.

## Parameters / Member Variables
- `strategy`: Partition strategy identifier (LIST, RANGE, HASH) that determines the pruning algorithm to use
- `partnatts`: Number of columns in the partition key, defining the dimensionality of the partition space
- `nparts`: Total number of partitions in this partitioned table
- `boundinfo`: PartitionBoundInfo structure containing partition boundary information for pruning decisions
- `partcollation`: Array of collation OIDs for each partition key column, used for string comparisons
- `partsupfunc`: Array of FmgrInfos for comparison or hashing functions associated with partition keys
- `stepcmpfuncs`: Array of FmgrInfos for comparison/hashing functions for each pruning step and partition key
- `ppccontext`: Memory context that holds this structure's subsidiary data and manages memory allocation
- `planstate`: Pointer to parent plan node's PlanState during execution; NULL when called from planner
- `exprcontext`: ExprContext for evaluating pruning expressions during execution
- `exprstates`: Array of ExprStates indexed by PruneCxtStateIdx, one for each partition key in each pruning step

## Dependencies
- Functions called/Symbols referenced:
  - [PartitionBoundInfo](PartitionBoundInfo.md)
  - [FmgrInfo](../F/FmgrInfo.md)
  - [MemoryContext](../M/MemoryContext.md)
  - [PlanState](PlanState.md)
  - [ExprContext](../E/ExprContext.md)
  - [ExprState](../E/ExprState.md)
  - Oid

- Called from (representative examples):
  - [InitPartitionPruneContext](../I/InitPartitionPruneContext.md) (src/backend/executor/execPartition.c:2097)
  - [prune_append_rel_partitions](../p/prune_append_rel_partitions.md) (src/backend/partitioning/partprune.c:755)
  - [get_matching_partitions](../g/get_matching_partitions.md) (src/backend/partitioning/partprune.c:817)
  - [get_matching_hash_bounds](../g/get_matching_hash_bounds.md) (src/backend/partitioning/partprune.c:2663)
  - [get_matching_list_bounds](../g/get_matching_list_bounds.md) (src/backend/partitioning/partprune.c:2740)
  - [get_matching_range_bounds](../g/get_matching_range_bounds.md) (src/backend/partitioning/partprune.c:2951)
  - [perform_pruning_base_step](../p/perform_pruning_base_step.md) (src/backend/partitioning/partprune.c:3416)
  - [perform_pruning_combine_step](../p/perform_pruning_combine_step.md) (src/backend/partitioning/partprune.c:3564)

## Notes and Other Information
- The structure is designed to work with the PruneCxtStateIdx enumeration that indexes into the exprstates array
- Memory management is handled through the ppccontext member to ensure proper cleanup
- The planstate member distinguishes between planner-time (NULL) and execution-time (non-NULL) usage
- [FmgrInfo](../F/FmgrInfo.md) arrays (partsupfunc, stepcmpfuncs) provide cached function call information for performance
- Used extensively in both the executor (execPartition.c) and partition pruning logic (partprune.c)
- Essential component of PostgreSQL's declarative partitioning performance optimization
- Supports complex partition pruning scenarios including multi-column partition keys and nested Boolean expressions