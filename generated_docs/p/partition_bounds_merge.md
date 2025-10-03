# partition_bounds_merge

## Location
[src/backend/partitioning/partbounds.c:1118-1197](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L1118-L1197)

## Overview
Determines if two relations can be joined partitionwise by checking if every partition matches/overlaps at most one partition on the other side, and builds merged partition bounds if possible.

## Definition

```c
PartitionBoundInfo
partition_bounds_merge(int partnatts,
					   FmgrInfo *partsupfunc, Oid *partcollation,
					   RelOptInfo *outer_rel, RelOptInfo *inner_rel,
					   JoinType jointype,
					   List **outer_parts, List **inner_parts)
```
## Detailed Description
This function analyzes the partition bounds of two relations to determine if they can be joined partitionwise. It checks whether every partition of the outer relation matches or overlaps at most one partition of the inner relation, and vice versa. If this condition is met, it builds the partition bounds for the join relation and generates lists of matching partition pairs.

The function delegates to strategy-specific implementations:
- Hash partitioning: Currently returns NULL (not supported)
- List partitioning: Calls merge_list_bounds
- Range partitioning: Calls merge_range_bounds

If any partition on one side matches multiple partitions on the other side, the function returns NULL and sets the output lists to NIL, indicating that partitioned join is not possible.

## Parameters / Member Variables
- `partnatts`: Number of partition attributes
- `*partsupfunc`: Array of comparison functions for partition attributes
- `*partcollation`: Array of collation OIDs for partition attributes
- `*outer_rel`: RelOptInfo for the outer relation in the join
- `*inner_rel`: RelOptInfo for the inner relation in the join
- `jointype`: Type of join being performed (INNER, LEFT, FULL, SEMI, ANTI)
- `**outer_parts`: Output parameter for list of matching outer partitions
- `**inner_parts`: Output parameter for list of matching inner partitions
## Dependencies
- Functions called/Symbols referenced:
  - [merge_list_bounds](../m/merge_list_bounds.md)
  - [merge_range_bounds](../m/merge_range_bounds.md)
- Data types used:
  - [PartitionBoundInfo](../P/PartitionBoundInfo.md)
  - [RelOptInfo](../R/RelOptInfo.md)
  - JoinType
  - [FmgrInfo](../F/FmgrInfo.md)
- Constants used:
  - JOIN_INNER, JOIN_LEFT, JOIN_FULL, JOIN_SEMI, JOIN_ANTI
  - PARTITION_STRATEGY_HASH, PARTITION_STRATEGY_LIST, PARTITION_STRATEGY_RANGE
- Called from:
  - [compute_partition_bounds](../c/compute_partition_bounds.md)
  - partition_bound_has_default

## Notes and Other Information
- Currently called only from try_partitionwise_join() for specific join types
- [Hash](../H/Hash.md) partitioning is not supported for partitioned joins due to complexity
- Both relations must use the same partitioning strategy
- Returns NULL if partitioned join is not feasible
- Output partition lists contain matching pairs at corresponding positions
- Critical for query optimizer's partitioned join planning

## Simplified Source

```c
PartitionBoundInfo
partition_bounds_merge(int partnatts,
                       FmgrInfo *partsupfunc, Oid *partcollation,
                       RelOptInfo *outer_rel, RelOptInfo *inner_rel,
                       JoinType jointype,
                       List **outer_parts, List **inner_parts)
{
    // Validate join type and partitioning strategy compatibility
    Assert(jointype == JOIN_INNER || jointype == JOIN_LEFT ||
           jointype == JOIN_FULL || jointype == JOIN_SEMI ||
           jointype == JOIN_ANTI);

    Assert(outer_rel->boundinfo->strategy == inner_rel->boundinfo->strategy);

    *outer_parts = *inner_parts = NIL;

    // Dispatch to strategy-specific merge implementation
    switch (outer_rel->boundinfo->strategy)
    {
        case PARTITION_STRATEGY_HASH:
            // Hash partitioning not supported for partitioned joins
            // due to complexity of handling different moduli
            return NULL;

        case PARTITION_STRATEGY_LIST:
            return merge_list_bounds(partsupfunc,
                                     partcollation,
                                     outer_rel,
                                     inner_rel,
                                     jointype,
                                     outer_parts,
                                     inner_parts);

        case PARTITION_STRATEGY_RANGE:
            return merge_range_bounds(partnatts,
                                      partsupfunc,
                                      partcollation,
                                      outer_rel,
                                      inner_rel,
                                      jointype,
                                      outer_parts,
                                      inner_parts);
    }

    return NULL;
}
```