# PartitionPruneFixSubPlanMap

## Location
src/backend/executor/execPartition.c: 2192 - 2302

## Overview
Fixes mapping of partition indexes to subplan indexes in PartitionPruneState by updating indexes to reflect the new subplan set after initial pruning has eliminated some subplans.

## Definition
```c
static void PartitionPruneFixSubPlanMap(PartitionPruneState *prunestate, Bitmapset *initially_valid_subplans, int n_total_subplans)
```

## Detailed Description
This function remaps subplan indexes within PartitionPruneState structures to account for subplans that were eliminated during initial pruning. When initial pruning removes some subplans, the remaining subplans get renumbered in a contiguous sequence. This function updates all the mapping arrays and bitmapsets within the pruning data structures to use the new subplan indexing scheme. It also rebuilds the present_parts bitmapsets for each partitioned relation, working from the lowest level upward to properly handle hierarchical partitioning where sub-partitioned tables may be entirely pruned.

## Parameters / Member Variables
- `prunestate`: PartitionPruneState structure containing mapping information to be updated
- `initially_valid_subplans`: Bitmapset of subplan indexes that survived initial pruning
- `n_total_subplans`: Total number of subplans before initial pruning occurred

## Dependencies
- Functions called/Symbols referenced:
  - bms_next_member
  - bms_free
  - bms_add_member
  - bms_is_empty
- Called from (representative examples):
  - ExecInitPartitionPruning

## Notes and Other Information
- Static function only accessible within execPartition.c
- Creates a temporary mapping array using 1-based indexing for convenience, with pruned items set to 0
- Processes partitioned relations in back-to-front order within each hierarchy to handle sub-partitioned tables correctly
- Rebuilds present_parts bitmapsets from scratch rather than trying to update them incrementally
- Updates both subplan_map arrays (for direct partition-to-subplan mapping) and other_subplans bitmapset
- Handles both regular partitions (with subplan_map entries) and sub-partitioned tables (with subpart_map entries)
- Essential for maintaining correct subplan references when subsequent pruning operations occur after initial pruning has changed the subplan landscape
- The function ensures that all internal data structures remain consistent with the post-pruning subplan numbering scheme