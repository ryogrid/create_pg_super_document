# get_merged_range_bounds

## Location
[src/backend/partitioning/partbounds.c:2711-2774](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L2711-L2774)

## Overview
Determines the bounds of a merged partition by combining the bounds of two range partitions based on the specified join type.

## Definition
```c
static void get_merged_range_bounds(int partnatts, FmgrInfo *partsupfuncs,
                                  Oid *partcollations, JoinType jointype,
                                  PartitionRangeBound *outer_lb,
                                  PartitionRangeBound *outer_ub,
                                  PartitionRangeBound *inner_lb,
                                  PartitionRangeBound *inner_ub,
                                  int lb_cmpval, int ub_cmpval,
                                  PartitionRangeBound *merged_lb,
                                  PartitionRangeBound *merged_ub)
```

## Detailed Description
This function computes the bounds of a merged partition that results from joining two range partitions. The logic varies based on the join type: INNER/SEMI joins produce the intersection of bounds (higher lower bound, lower upper bound), LEFT/ANTI joins preserve the outer partition's bounds, and FULL joins produce the union of bounds (lower lower bound, higher upper bound). The function uses pre-computed comparison values to efficiently determine which bounds to use without re-comparing them.

## Parameters / Member Variables
- `partnatts`: Number of partition key attributes
- `partsupfuncs`: Array of comparison functions for partition key types
- `partcollations`: Array of collation OIDs for partition key attributes
- `jointype`: Type of join operation (INNER, SEMI, LEFT, ANTI, FULL)
- `outer_lb`: Lower bound of the outer (first) partition
- `outer_ub`: Upper bound of the outer (first) partition
- `inner_lb`: Lower bound of the inner (second) partition
- `inner_ub`: Upper bound of the inner (second) partition
- `lb_cmpval`: Pre-computed comparison result for lower bounds
- `ub_cmpval`: Pre-computed comparison result for upper bounds
- `merged_lb`: Output parameter for merged partition's lower bound
- `merged_ub`: Output parameter for merged partition's upper bound

## Dependencies
- Functions called/Symbols referenced:
  - compare_range_bounds
  - JoinType
  - [PartitionRangeBound](../P/PartitionRangeBound.md)
  - JOIN_INNER, JOIN_SEMI, JOIN_LEFT, JOIN_ANTI, JOIN_FULL
- Called from (representative examples):
  - [merge_range_bounds](../m/merge_range_bounds.md)

## Notes and Other Information
- This is a static function within partbounds.c used for partition-wise join optimization
- The function includes assertions to verify that the provided comparison values are consistent
- Different join types require different merging strategies to preserve join semantics
- Critical for PostgreSQL's partition-wise join feature which can significantly improve query performance
- Located in src/backend/partitioning/partbounds.c:2711-2774

## Simplified Source

```c
static void
get_merged_range_bounds(int partnatts, FmgrInfo *partsupfuncs,
                       Oid *partcollations, JoinType jointype,
                       PartitionRangeBound *outer_lb,
                       PartitionRangeBound *outer_ub,
                       PartitionRangeBound *inner_lb,
                       PartitionRangeBound *inner_ub,
                       int lb_cmpval, int ub_cmpval,
                       PartitionRangeBound *merged_lb,
                       PartitionRangeBound *merged_ub)
{
    switch (jointype)
    {
        case JOIN_INNER:
        case JOIN_SEMI:
            // INNER/SEMI: intersection (higher lower, lower upper)
            *merged_lb = (lb_cmpval > 0) ? *outer_lb : *inner_lb;
            *merged_ub = (ub_cmpval < 0) ? *outer_ub : *inner_ub;
            break;

        case JOIN_LEFT:
        case JOIN_ANTI:
            // LEFT/ANTI: preserve outer bounds
            *merged_lb = *outer_lb;
            *merged_ub = *outer_ub;
            break;

        case JOIN_FULL:
            // FULL: union (lower lower, higher upper)
            *merged_lb = (lb_cmpval < 0) ? *outer_lb : *inner_lb;
            *merged_ub = (ub_cmpval > 0) ? *outer_ub : *inner_ub;
            break;

        default:
            elog(ERROR, "unrecognized join type: %d", (int) jointype);
    }
}
```