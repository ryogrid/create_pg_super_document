# try_partitionwise_join

## Location
[src/backend/optimizer/path/joinrels.c:1479-1693](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/joinrels.c#L1479-L1693)

## Overview
Attempts to perform partitionwise join optimization by breaking down a join between two partitioned relations into joins between matching partitions.

## Definition
static void try_partitionwise_join(PlannerInfo *root, RelOptInfo *rel1, RelOptInfo *rel2, RelOptInfo *joinrel, SpecialJoinInfo *parent_sjinfo, List *parent_restrictlist)

## Detailed Description
This function implements the core logic for partitionwise join optimization, a technique that can significantly improve join performance when joining partitioned tables. The function determines whether a join between two partitioned relations can be decomposed into separate joins between corresponding partitions.

Partitionwise join is possible when:
1. Both joining relations have the same partitioning scheme
2. There exists an equi-join between the partition keys of the two relations

The function works in two main phases:
1. Creates RelOptInfos for joins between matching partitions (child-joins) and adds paths to them
2. Later, Append or MergeAppend paths are constructed across the set of child joins (by generate_partitionwise_join_paths)

For each partition pair, the function:
- Checks if the partition segments can be safely ignored (e.g., empty partitions)
- Constructs child SpecialJoinInfo and restrictlist structures by translating parent structures
- Creates or finds child join RelOptInfo structures
- Populates the child joins with appropriate paths

## Parameters / Member Variables
- : PlannerInfo containing global planner state and context
- : First partitioned relation to join
- : Second partitioned relation to join  
- : The target join relation that will contain the partitionwise join
- : SpecialJoinInfo for the parent join operation
- : List of join restriction clauses from the parent join

## Dependencies
- Functions called/Symbols referenced:
  - [compute_partition_bounds](../c/compute_partition_bounds.md)
  - [build_child_join_sjinfo](../b/build_child_join_sjinfo.md)
  - [free_child_join_sjinfo](../f/free_child_join_sjinfo.md)
  - build_child_join_rel
  - [populate_joinrel_with_paths](../p/populate_joinrel_with_paths.md)
  - [find_appinfos_by_relids](../f/find_appinfos_by_relids.md)
  - [adjust_appendrel_attrs](../a/adjust_appendrel_attrs.md)
  - [adjust_child_relids](../a/adjust_child_relids.md)
  - IS_PARTITIONED_REL
  - IS_SIMPLE_REL
  - IS_DUMMY_REL
- Called from (representative examples):
  - [populate_joinrel_with_paths](../p/populate_joinrel_with_paths.md)

## Notes and Other Information
- Guards against stack overflow due to overly deep partition hierarchies
- Handles various join types (INNER, LEFT, FULL, SEMI, ANTI) with appropriate empty partition logic
- Fails gracefully when partitionwise join is not feasible by setting joinrel->nparts = 0
- Maintains partition bounds information and live partition tracking
- Uses AppendRelInfo structures to translate expressions between parent and child relations
- Part of PostgreSQL's advanced join optimization framework for partitioned tables

## Simplified Source

```c
static void try_partitionwise_join(PlannerInfo *root, RelOptInfo *rel1, RelOptInfo *rel2,
                                  RelOptInfo *joinrel, SpecialJoinInfo *parent_sjinfo,
                                  List *parent_restrictlist) {
    bool rel1_is_simple = IS_SIMPLE_REL(rel1);
    bool rel2_is_simple = IS_SIMPLE_REL(rel2);
    List *parts1 = NIL, *parts2 = NIL;
    ListCell *lcr1 = NULL, *lcr2 = NULL;
    int cnt_parts;

    check_stack_depth();

    // Early exit conditions
    if (joinrel->part_scheme == NULL || joinrel->nparts == 0)
        return;

    if (!IS_PARTITIONED_REL(rel1) || !IS_PARTITIONED_REL(rel2))
        return;

    // Compute partition bounds for matching partitions
    compute_partition_bounds(root, rel1, rel2, joinrel, parent_sjinfo, &parts1, &parts2);

    if (joinrel->partbounds_merged) {
        lcr1 = list_head(parts1);
        lcr2 = list_head(parts2);
    }

    // Process each partition pair
    for (cnt_parts = 0; cnt_parts < joinrel->nparts; cnt_parts++) {
        RelOptInfo *child_rel1, *child_rel2;
        bool rel1_empty, rel2_empty;

        // Get child relations for this partition
        if (joinrel->partbounds_merged) {
            child_rel1 = lfirst_node(RelOptInfo, lcr1);
            child_rel2 = lfirst_node(RelOptInfo, lcr2);
            lcr1 = lnext(parts1, lcr1);
            lcr2 = lnext(parts2, lcr2);
        } else {
            child_rel1 = rel1->part_rels[cnt_parts];
            child_rel2 = rel2->part_rels[cnt_parts];
        }

        rel1_empty = (child_rel1 == NULL || IS_DUMMY_REL(child_rel1));
        rel2_empty = (child_rel2 == NULL || IS_DUMMY_REL(child_rel2));

        // Skip empty partition segments based on join type
        switch (parent_sjinfo->jointype) {
            case JOIN_INNER:
            case JOIN_SEMI:
                if (rel1_empty || rel2_empty) continue;
                break;
            case JOIN_LEFT:
            case JOIN_ANTI:
                if (rel1_empty) continue;
                break;
            case JOIN_FULL:
                if (rel1_empty && rel2_empty) continue;
                break;
        }

        // Fail if child partitions are completely pruned
        if (child_rel1 == NULL || child_rel2 == NULL) {
            joinrel->nparts = 0;
            return;
        }

        // Fail if dummy relations don't support partitionwise join
        if ((rel1_is_simple && !child_rel1->consider_partitionwise_join) ||
            (rel2_is_simple && !child_rel2->consider_partitionwise_join)) {
            joinrel->nparts = 0;
            return;
        }

        // Build child join structures
        SpecialJoinInfo *child_sjinfo = build_child_join_sjinfo(root, parent_sjinfo,
                                                               child_rel1->relids,
                                                               child_rel2->relids);

        AppendRelInfo **appinfos;
        int nappinfos;
        appinfos = find_appinfos_by_relids(root,
                                          bms_union(child_rel1->relids, child_rel2->relids),
                                          &nappinfos);

        List *child_restrictlist = (List *) adjust_appendrel_attrs(root,
                                                                  (Node *) parent_restrictlist,
                                                                  nappinfos, appinfos);

        // Create or find child join relation
        RelOptInfo *child_joinrel = joinrel->part_rels[cnt_parts];
        if (!child_joinrel) {
            child_joinrel = build_child_join_rel(root, child_rel1, child_rel2,
                                                joinrel, child_restrictlist, child_sjinfo);
            joinrel->part_rels[cnt_parts] = child_joinrel;
            joinrel->live_parts = bms_add_member(joinrel->live_parts, cnt_parts);
            joinrel->all_partrels = bms_add_members(joinrel->all_partrels,
                                                   child_joinrel->relids);
        }

        // Generate paths for the child join
        populate_joinrel_with_paths(root, child_rel1, child_rel2, child_joinrel,
                                   child_sjinfo, child_restrictlist);

        // Clean up
        pfree(appinfos);
        free_child_join_sjinfo(child_sjinfo, parent_sjinfo);
    }
}
```