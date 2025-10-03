# PartitionPruneFixSubPlanMap

## Location
[src/backend/executor/execPartition.c:2192-2302](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execPartition.c#L2192-L2302)

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
  - [bms_next_member](../b/bms_next_member.md)
  - [bms_free](../b/bms_free.md)
  - [bms_add_member](../b/bms_add_member.md)
  - bms_is_empty
- Called from (representative examples):
  - [ExecInitPartitionPruning](../E/ExecInitPartitionPruning.md)

## Notes and Other Information
- Static function only accessible within execPartition.c
- Creates a temporary mapping array using 1-based indexing for convenience, with pruned items set to 0
- Processes partitioned relations in back-to-front order within each hierarchy to handle sub-partitioned tables correctly
- Rebuilds present_parts bitmapsets from scratch rather than trying to update them incrementally
- Updates both subplan_map arrays (for direct partition-to-subplan mapping) and other_subplans bitmapset
- Handles both regular partitions (with subplan_map entries) and sub-partitioned tables (with subpart_map entries)
- Essential for maintaining correct subplan references when subsequent pruning operations occur after initial pruning has changed the subplan landscape
- The function ensures that all internal data structures remain consistent with the post-pruning subplan numbering scheme

## Simplified Source

```c
static void
PartitionPruneFixSubPlanMap(PartitionPruneState *prunestate,
                           Bitmapset *initially_valid_subplans,
                           int n_total_subplans)
{
    int *new_subplan_indexes;
    Bitmapset *new_other_subplans;

    // Build mapping array from old subplan indexes to new ones
    // Use 1-based indexing for convenience, pruned items stay 0
    new_subplan_indexes = (int *) palloc0(sizeof(int) * n_total_subplans);

    int newidx = 1;
    int i = -1;
    while ((i = bms_next_member(initially_valid_subplans, i)) >= 0)
    {
        Assert(i < n_total_subplans);
        new_subplan_indexes[i] = newidx++;
    }

    // Update each partition hierarchy's subplan mappings
    for (int i = 0; i < prunestate->num_partprunedata; i++)
    {
        PartitionPruningData *prunedata = prunestate->partprunedata[i];

        // Process relations in reverse order (lowest level first)
        // This ensures sub-partitioned tables are handled correctly
        for (int j = prunedata->num_partrelprunedata - 1; j >= 0; j--)
        {
            PartitionedRelPruningData *pprune = &prunedata->partrelprunedata[j];
            int nparts = pprune->nparts;

            // Rebuild present_parts from scratch
            bms_free(pprune->present_parts);
            pprune->present_parts = NULL;

            // Update mapping for each partition
            for (int k = 0; k < nparts; k++)
            {
                int oldidx = pprune->subplan_map[k];

                if (oldidx >= 0)
                {
                    // Regular partition with direct subplan mapping
                    Assert(oldidx < n_total_subplans);
                    pprune->subplan_map[k] = new_subplan_indexes[oldidx] - 1;

                    // Add to present_parts if subplan survived pruning
                    if (new_subplan_indexes[oldidx] > 0)
                        pprune->present_parts = bms_add_member(pprune->present_parts, k);
                }
                else
                {
                    // Sub-partitioned table - check if any sub-partitions remain
                    int subidx = pprune->subpart_map[k];
                    if (subidx >= 0)
                    {
                        PartitionedRelPruningData *subprune = &prunedata->partrelprunedata[subidx];

                        if (!bms_is_empty(subprune->present_parts))
                            pprune->present_parts = bms_add_member(pprune->present_parts, k);
                    }
                }
            }
        }
    }

    // Update other_subplans bitmap with new indexes
    new_other_subplans = NULL;
    i = -1;
    while ((i = bms_next_member(prunestate->other_subplans, i)) >= 0)
    {
        new_other_subplans = bms_add_member(new_other_subplans,
                                           new_subplan_indexes[i] - 1);
    }

    // Replace old other_subplans with updated version
    bms_free(prunestate->other_subplans);
    prunestate->other_subplans = new_other_subplans;

    pfree(new_subplan_indexes);
}
```