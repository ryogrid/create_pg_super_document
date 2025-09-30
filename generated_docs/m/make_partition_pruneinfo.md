# make_partition_pruneinfo

## Location
[src/backend/partitioning/partprune.c:220-391](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partprune.c#L220-L391)

## Overview
Builds a PartitionPruneInfo structure that enables additional partition pruning during query execution, returning NULL when partition pruning would be ineffective.

## Definition

```c
struct a temporary array to map from partition-child-relation
	 * relid to the index in 'subpaths' of the scan plan for that partition.
	 * (Use of "subplan" rather than "subpath" is a bit of a misnomer, but
	 * we'll let it stand.)  For convenience, we use 1-based indexes here, so
	 * that zero can represent an un-filled array entry.
	 */
	allpartrelids = NIL;
```
## Detailed Description
This function constructs a PartitionPruneInfo data structure that the executor can use to perform runtime partition pruning. It analyzes the provided subpaths to identify partition child relations and their parent partitioned relations, then builds pruning information for each relevant partition hierarchy.

The function works by:
1. Scanning subpaths to identify partition child relations and their partitioned parent relations
2. Creating a mapping from partition child relation IDs to subplan indexes
3. Traversing up partition hierarchies to collect all relevant partitioned relations
4. Building PartitionedRelPruneInfo structures for each top-level partitioned relation
5. Identifying subplans that don't belong to any partitioned relation (for UNION ALL with non-partitioned tables)

The function restricts parent partitioned relations to be either the parentrel or children of parentrel to ensure that prunequal clauses can be properly translated.

## Parameters / Member Variables
- : PlannerInfo containing global planning state and relation information
- : RelOptInfo for the appendrel being processed
- : List of scan paths for the appendrel's child relations
- : List of potential pruning qualification clauses applicable to the appendrel

## Dependencies
- Functions called/Symbols referenced:
  - [add_part_relids](../a/add_part_relids.md)
  - [make_partitionedrel_pruneinfo](make_partitionedrel_pruneinfo.md)
  - [find_base_rel](../f/find_base_rel.md)
  - IS_PARTITIONED_REL
  - [bms_add_member](../b/bms_add_member.md)
  - [bms_join](../b/bms_join.md)
  - [bms_num_members](../b/bms_num_members.md)
  - [bms_add_range](../b/bms_add_range.md)
  - [bms_del_members](../b/bms_del_members.md)
- Called from (representative examples):
  - [create_append_plan](../c/create_append_plan.md)
  - [create_merge_append_plan](../c/create_merge_append_plan.md)

## Notes and Other Information
- Returns NULL when no useful runtime pruning can be performed
- Handles multi-level partitioning hierarchies by traversing up to topmost partitioned parents
- Creates a complement bitmapset for subplans that don't belong to any partitioned relation
- Uses 1-based indexing in the relid_subplan_map for convenience (zero represents unfilled entries)
- Properly handles partitionwise joins of multi-level partitioning trees where parentrel may be an intermediate partitioned table

## Simplified Source

```c
PartitionPruneInfo *
make_partition_pruneinfo(PlannerInfo *root, RelOptInfo *parentrel,
                         List *subpaths, List *prunequal)
{
    PartitionPruneInfo *pruneinfo;
    Bitmapset *allmatchedsubplans = NULL;
    List *allpartrelids = NIL;
    List *prunerelinfos = NIL;
    int *relid_subplan_map;

    // Create mapping from relation ID to subplan index
    relid_subplan_map = palloc0(sizeof(int) * root->simple_rel_array_size);

    // Scan subpaths to identify partition child relations
    int i = 1;
    foreach(lc, subpaths) {
        Path *path = (Path *) lfirst(lc);
        RelOptInfo *pathrel = path->parent;

        // Process partition member relations
        if (pathrel->reloptkind == RELOPT_OTHER_MEMBER_REL) {
            // Traverse up partition hierarchy to collect parent relids
            Bitmapset *partrelids = collect_partition_parents(pathrel, parentrel, root);

            if (partrelids) {
                allpartrelids = add_part_relids(allpartrelids, partrelids);
                relid_subplan_map[pathrel->relid] = i;
            }
        }
        i++;
    }

    // Build pruning info for each partitioned relation hierarchy
    foreach(lc, allpartrelids) {
        Bitmapset *partrelids = (Bitmapset *) lfirst(lc);
        List *pinfolist;
        Bitmapset *matchedsubplans = NULL;

        pinfolist = make_partitionedrel_pruneinfo(root, parentrel, prunequal,
                                                  partrelids, relid_subplan_map,
                                                  &matchedsubplans);

        if (pinfolist != NIL) {
            prunerelinfos = lappend(prunerelinfos, pinfolist);
            allmatchedsubplans = bms_join(matchedsubplans, allmatchedsubplans);
        }
    }

    pfree(relid_subplan_map);

    // Return NULL if no useful pruning information found
    if (prunerelinfos == NIL)
        return NULL;

    // Build result structure
    pruneinfo = makeNode(PartitionPruneInfo);
    pruneinfo->prune_infos = prunerelinfos;

    // Identify subplans that don't belong to any partitioned relation
    if (bms_num_members(allmatchedsubplans) < list_length(subpaths)) {
        Bitmapset *other_subplans = bms_add_range(NULL, 0, list_length(subpaths) - 1);
        other_subplans = bms_del_members(other_subplans, allmatchedsubplans);
        pruneinfo->other_subplans = other_subplans;
    } else {
        pruneinfo->other_subplans = NULL;
    }

    return pruneinfo;
}
```