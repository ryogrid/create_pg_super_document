# compute_partition_bounds

## Location
[src/backend/optimizer/path/joinrels.c:1790-1880](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/joinrels.c#L1790-L1880)

## Overview
Computes the partition bounds for a join relation based on the partition bounds of its input relations, determining how partitions should be paired for partitionwise joins.

## Definition
static void compute_partition_bounds(PlannerInfo *root, RelOptInfo *rel1, RelOptInfo *rel2, RelOptInfo *joinrel, SpecialJoinInfo *parent_sjinfo, List **parts1, List **parts2)

## Detailed Description
This function establishes the partition boundaries and pairing strategy for a partitioned join relation. It handles two main scenarios:

**First-time computation (joinrel->nparts == -1):**
1. **Identical bounds optimization**: If both input relations have identical partition bounds (not merged, same number of partitions, and equivalent boundary values), the join relation inherits these bounds directly. Partitions are paired by cardinal position.

2. **Merged bounds computation**: When input relations have different partition bounds, the function calls partition_bounds_merge() to create new merged bounds that accommodate both input partitioning schemes. This sets the partbounds_merged flag and creates a mapping between input and output partitions.

**Subsequent calls:**
- For relations with merged bounds, calls get_matching_part_pairs() to determine the correct partition pairings based on the merged boundary structure.
- For relations with identical bounds, partition pairing is assumed to follow cardinal positions.

The function also initializes the joinrel's partition-related data structures including the part_rels array that will hold child join RelOptInfo structures.

## Parameters / Member Variables
- : PlannerInfo containing global planner state and context
- : First input relation for the join operation
- : Second input relation for the join operation
- : Target join relation that will contain the computed partition bounds
- : SpecialJoinInfo containing join type and other join-specific information
- : Output parameter - list of rel1's partitions paired for joining
- : Output parameter - list of rel2's partitions paired for joining

## Dependencies
- Functions called/Symbols referenced:
  - [partition_bounds_equal](../p/partition_bounds_equal.md)
  - [partition_bounds_merge](../p/partition_bounds_merge.md)
  - [get_matching_part_pairs](../g/get_matching_part_pairs.md)
  - [list_length](../l/list_length.md)
  - [palloc0](../p/palloc0.md)
  - Assert
  - [PartitionScheme](../P/PartitionScheme.md)
  - [PartitionBoundInfo](../P/PartitionBoundInfo.md)
- Called from (representative examples):
  - [try_partitionwise_join](../t/try_partitionwise_join.md)

## Notes and Other Information
- Sets joinrel->nparts to 0 if partition bounds cannot be merged (incompatible partitioning)
- Uses the partbounds_merged flag to track whether bounds required merging vs. direct inheritance
- Optimizes for the common case where input relations have identical partitioning schemes
- Allocates part_rels array sized to hold RelOptInfo pointers for all partition combinations
- Critical for determining feasibility and strategy of partitionwise join optimization
- Handles various partitioning scenarios including hash, range, and list partitioning schemes

## Simplified Source

```c
static void compute_partition_bounds(PlannerInfo *root, RelOptInfo *rel1,
                                   RelOptInfo *rel2, RelOptInfo *joinrel,
                                   SpecialJoinInfo *parent_sjinfo,
                                   List **parts1, List **parts2) {
    // Only compute if not already done
    if (joinrel->nparts == -1) {
        PartitionScheme part_scheme = joinrel->part_scheme;
        PartitionBoundInfo boundinfo = NULL;
        int nparts = 0;

        // Optimization: check if both relations have identical bounds
        if (!rel1->partbounds_merged && !rel2->partbounds_merged &&
            rel1->nparts == rel2->nparts &&
            partition_bounds_equal(part_scheme->partnatts,
                                  part_scheme->parttyplen,
                                  part_scheme->parttypbyval,
                                  rel1->boundinfo, rel2->boundinfo)) {
            // Identical bounds - inherit directly
            boundinfo = rel1->boundinfo;
            nparts = rel1->nparts;
        } else {
            // Different bounds - merge them
            boundinfo = partition_bounds_merge(part_scheme->partnatts,
                                              part_scheme->partsupfunc,
                                              part_scheme->partcollation,
                                              rel1, rel2,
                                              parent_sjinfo->jointype,
                                              parts1, parts2);
            if (boundinfo == NULL) {
                joinrel->nparts = 0;  // Cannot merge
                return;
            }
            nparts = list_length(*parts1);
            joinrel->partbounds_merged = true;
        }

        // Set up join relation partition info
        joinrel->boundinfo = boundinfo;
        joinrel->nparts = nparts;
        joinrel->part_rels = (RelOptInfo **) palloc0(sizeof(RelOptInfo *) * nparts);

    } else {
        // Already computed - just get partition pairs if needed
        if (joinrel->partbounds_merged) {
            get_matching_part_pairs(root, joinrel, rel1, rel2, parts1, parts2);
        }
    }
}
```