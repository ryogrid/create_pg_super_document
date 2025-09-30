# make_partitionedrel_pruneinfo

## Location
[src/backend/partitioning/partprune.c:438-713](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partprune.c#L438-L713)

## Overview
Builds a list of PartitionedRelPruneInfo structures for each partitioned relation in a hierarchy, enabling runtime partition pruning during query execution.

## Definition
```c
static List *make_partitionedrel_pruneinfo(PlannerInfo *root, RelOptInfo *parentrel, List *prunequal, Bitmapset *partrelids, int *relid_subplan_map, Bitmapset **matchedsubplans)
```

## Detailed Description
This function constructs detailed pruning information for each partitioned relation within a single partitioning hierarchy. It performs a two-phase process:

**Phase 1**: Examines each partitioned relation to determine if runtime pruning is needed and constructs basic PartitionedRelPruneInfo structures. It translates pruning qualifications for each partition level and generates both initial (startup) and execution-time pruning steps using gen_partprune_steps.

**Phase 2**: If runtime pruning is required, builds the mapping structures needed by the executor:
- subplan_map: Maps partition indexes to subplan indexes for leaf partitions
- subpart_map: Maps partition indexes to PartitionedRelPruneInfo indexes for sub-partitioned relations
- relid_map: Maps partition indexes to relation OIDs

The function handles multi-level partitioning by properly translating qualifications between different levels using adjust_appendrel_attrs and adjust_appendrel_attrs_multilevel.

## Parameters / Member Variables
- `root`: PlannerInfo containing global planning state and relation information  
- `parentrel`: RelOptInfo associated with the appendpath being considered
- `prunequal`: List of potential pruning qualifications represented for parentrel
- `partrelids`: Bitmapset of RT indexes identifying relevant partitioned tables within the hierarchy
- `relid_subplan_map`: Array mapping child relation relids to subplan indexes
- `matchedsubplans`: Output parameter receiving the set of matched subplan indexes

## Dependencies
- Functions called/Symbols referenced:
  - [gen_partprune_steps](../g/gen_partprune_steps.md)
  - [find_base_rel](../f/find_base_rel.md)
  - [adjust_appendrel_attrs](../a/adjust_appendrel_attrs.md)
  - [adjust_appendrel_attrs_multilevel](../a/adjust_appendrel_attrs_multilevel.md)
  - [find_appinfos_by_relids](../f/find_appinfos_by_relids.md)
  - [get_partkey_exec_paramids](../g/get_partkey_exec_paramids.md)
  - [bms_next_member](../b/bms_next_member.md)
  - [bms_equal](../b/bms_equal.md)
  - bms_is_empty
  - [bms_add_member](../b/bms_add_member.md)
  - planner_rt_fetch
- Called from (representative examples):
  - [make_partition_pruneinfo](make_partition_pruneinfo.md)

## Notes and Other Information
- Returns NIL if no useful runtime pruning steps can be generated
- Detects contradictory qualifications and disables runtime pruning if found
- Distinguishes between startup pruning (for mutable operators/expressions) and per-scan pruning (for exec parameters)
- Uses 1-based indexing for relid_subpart_map and converts to 0-based indexing for final maps
- Handles cases where parentrel and target partition may have different column orders in sub-partitioned tables
- Creates present_parts bitmapset to track which partitions are available (not already pruned)
- The function is static and only used within the partition pruning subsystem

## Simplified Source

```c
static List *
make_partitionedrel_pruneinfo(PlannerInfo *root, RelOptInfo *parentrel,
                             List *prunequal, Bitmapset *partrelids,
                             int *relid_subplan_map, Bitmapset **matchedsubplans)
{
    RelOptInfo *targetpart = NULL;
    List       *pinfolist = NIL;
    bool        doruntimeprune = false;
    int        *relid_subpart_map;
    Bitmapset  *subplansfound = NULL;
    int         rti;
    int         i;

    // Phase 1: Examine each partitioned rel and determine if runtime pruning is needed
    relid_subpart_map = palloc0(sizeof(int) * root->simple_rel_array_size);

    i = 1;
    rti = -1;
    while ((rti = bms_next_member(partrelids, rti)) > 0)
    {
        RelOptInfo *subpart = find_base_rel(root, rti);
        PartitionedRelPruneInfo *pinfo;
        List       *partprunequal;
        List       *initial_pruning_steps;
        List       *exec_pruning_steps;
        Bitmapset  *execparamids;
        GeneratePruningStepsContext context;

        // Fill the mapping array (1-based indexes)
        relid_subpart_map[rti] = i++;

        // Translate pruning qual for this partition
        if (!targetpart)
        {
            targetpart = subpart;

            // Adjust quals if parent and target are different
            if (!bms_equal(parentrel->relids, subpart->relids))
            {
                int nappinfos;
                AppendRelInfo **appinfos = find_appinfos_by_relids(root,
                                                                  subpart->relids,
                                                                  &nappinfos);
                prunequal = (List *) adjust_appendrel_attrs(root, (Node *) prunequal,
                                                           nappinfos, appinfos);
                pfree(appinfos);
            }
            partprunequal = prunequal;
        }
        else
        {
            // For sub-partitioned tables, translate quals to match column order
            partprunequal = (List *)
                adjust_appendrel_attrs_multilevel(root, (Node *) prunequal,
                                                 subpart, targetpart);
        }

        // Generate pruning steps for startup pruning
        gen_partprune_steps(subpart, partprunequal, PARTTARGET_INITIAL, &context);

        if (context.contradictory)
            return NIL;  // Disable runtime pruning on contradictions

        // Create initial pruning steps if we have mutable operators/expressions
        if (context.has_mutable_op || context.has_mutable_arg)
            initial_pruning_steps = context.steps;
        else
            initial_pruning_steps = NIL;

        // Generate exec pruning steps if we have exec parameters
        if (context.has_exec_param)
        {
            gen_partprune_steps(subpart, partprunequal, PARTTARGET_EXEC, &context);

            if (context.contradictory)
                return NIL;

            exec_pruning_steps = context.steps;
            execparamids = get_partkey_exec_paramids(exec_pruning_steps);

            if (bms_is_empty(execparamids))
                exec_pruning_steps = NIL;
        }
        else
        {
            exec_pruning_steps = NIL;
            execparamids = NULL;
        }

        if (initial_pruning_steps || exec_pruning_steps)
            doruntimeprune = true;

        // Create PartitionedRelPruneInfo for this rel
        pinfo = makeNode(PartitionedRelPruneInfo);
        pinfo->rtindex = rti;
        pinfo->initial_pruning_steps = initial_pruning_steps;
        pinfo->exec_pruning_steps = exec_pruning_steps;
        pinfo->execparamids = execparamids;

        pinfolist = lappend(pinfolist, pinfo);
    }

    if (!doruntimeprune)
    {
        pfree(relid_subpart_map);
        return NIL;  // No runtime pruning needed
    }

    // Phase 2: Build mapping structures for executor
    foreach(lc, pinfolist)
    {
        PartitionedRelPruneInfo *pinfo = lfirst(lc);
        RelOptInfo *subpart = find_base_rel(root, pinfo->rtindex);
        Bitmapset  *present_parts;
        int         nparts = subpart->nparts;
        int        *subplan_map;
        int        *subpart_map;
        Oid        *relid_map;

        // Create maps (convert to 0-based indexing with -1 for empty entries)
        subplan_map = (int *) palloc(nparts * sizeof(int));
        memset(subplan_map, -1, nparts * sizeof(int));
        subpart_map = (int *) palloc(nparts * sizeof(int));
        memset(subpart_map, -1, nparts * sizeof(int));
        relid_map = (Oid *) palloc0(nparts * sizeof(Oid));
        present_parts = NULL;

        // Fill maps for live partitions
        i = -1;
        while ((i = bms_next_member(subpart->live_parts, i)) >= 0)
        {
            RelOptInfo *partrel = subpart->part_rels[i];
            int         subplanidx;
            int         subpartidx;

            subplan_map[i] = subplanidx = relid_subplan_map[partrel->relid] - 1;
            subpart_map[i] = subpartidx = relid_subpart_map[partrel->relid] - 1;
            relid_map[i] = planner_rt_fetch(partrel->relid, root)->relid;

            if (subplanidx >= 0)
            {
                present_parts = bms_add_member(present_parts, i);
                subplansfound = bms_add_member(subplansfound, subplanidx);
            }
            else if (subpartidx >= 0)
                present_parts = bms_add_member(present_parts, i);
        }

        // Store the maps in PartitionedRelPruneInfo
        pinfo->present_parts = present_parts;
        pinfo->nparts = nparts;
        pinfo->subplan_map = subplan_map;
        pinfo->subpart_map = subpart_map;
        pinfo->relid_map = relid_map;
    }

    pfree(relid_subpart_map);
    *matchedsubplans = subplansfound;
    return pinfolist;
}
```