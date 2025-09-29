# preprocess_grouping_sets

## Location
[src/backend/optimizer/plan/planner.c:2077-2257](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L2077-L2257)

## Overview
Performs preprocessing for GROUPING SETS clauses by expanding grouping sets, organizing them into rollup structures, and preparing annotations for cost estimation.

## Definition
```c
static grouping_sets_data *preprocess_grouping_sets(PlannerInfo *root)
```

## Detailed Description
This function is responsible for the complex preprocessing of GROUPING SETS clauses in PostgreSQL. It handles the transformation from the raw parse tree representation into organized structures suitable for execution planning.

Key operations include:
1. **Expansion**: Uses expand_grouping_sets to expand complex grouping set specifications
2. **Classification**: Separates columns into hashable/unhashable and sortable/unsortable categories
3. **Validation**: Ensures that unsortable grouping sets are still hashable (required constraint)
4. **Organization**: Groups related grouping sets into rollups for efficient execution
5. **Reordering**: Orders grouping sets optimally, considering ORDER BY clauses when possible
6. **Mapping**: Creates index mappings from sort group references to column positions

The function creates a grouping_sets_data structure containing all the information needed by later planning phases, including rollup data for sortable sets and separate handling for unsortable (hash-only) sets.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing the query planning context

## Dependencies
- Functions called/Symbols referenced:
  - [expand_grouping_sets](../e/expand_grouping_sets.md), extract_rollup_sets, reorder_grouping_sets
  - [preprocess_groupclause](preprocess_groupclause.md), remap_to_groupclause_idx
  - [bms_add_member](../b/bms_add_member.md), bms_overlap_list, bms_is_empty
  - makeNode (GroupingSetData, RollupData)
- Called from (representative examples):
  - [grouping_planner](../g/grouping_planner.md)

## Notes and Other Information
- Located in src/backend/optimizer/plan/planner.c:2077-2257
- This is a static function that serves as a key component of GROUPING SETS planning
- The function enforces the constraint that unsortable sets must be hashable, throwing an error if violated
- When only one aggregation pass is needed, the function tries to match the ORDER BY clause for efficiency
- Creates separate handling paths for sortable sets (organized into rollups) and unsortable sets (hash-only)
- The tleref_to_colnum_map workspace array is used for remapping sort group references to column indices
- Sets processed_groupClause to the original groupClause when grouping sets are present (no optimization currently performed)

## Simplified Source

```c
static grouping_sets_data *
preprocess_grouping_sets(PlannerInfo *root)
{
    Query *parse = root->parse;
    List *sets;
    int maxref = 0;
    grouping_sets_data *gd = palloc0(sizeof(grouping_sets_data));

    // Expand grouping sets specification
    parse->groupingSets = expand_grouping_sets(parse->groupingSets,
                                               parse->groupDistinct, -1);

    // Initialize tracking variables
    gd->any_hashable = false;
    gd->unhashable_refs = NULL;
    gd->unsortable_refs = NULL;
    gd->unsortable_sets = NIL;

    // No optimization for groupClause when grouping sets present
    root->processed_groupClause = parse->groupClause;

    // Analyze group clauses for hashability and sortability
    if (parse->groupClause) {
        foreach(lc, parse->groupClause) {
            SortGroupClause *gc = lfirst_node(SortGroupClause, lc);
            Index ref = gc->tleSortGroupRef;

            if (ref > maxref)
                maxref = ref;

            if (!gc->hashable)
                gd->unhashable_refs = bms_add_member(gd->unhashable_refs, ref);

            if (!OidIsValid(gc->sortop))
                gd->unsortable_refs = bms_add_member(gd->unsortable_refs, ref);
        }
    }

    // Allocate workspace for mapping
    gd->tleref_to_colnum_map = palloc((maxref + 1) * sizeof(int));

    // Handle unsortable sets separately
    if (!bms_is_empty(gd->unsortable_refs)) {
        List *sortable_sets = NIL;

        foreach(lc, parse->groupingSets) {
            List *gset = (List *) lfirst(lc);

            if (bms_overlap_list(gd->unsortable_refs, gset)) {
                // Unsortable set - must be hashable
                GroupingSetData *gs = makeNode(GroupingSetData);
                gs->set = gset;
                gd->unsortable_sets = lappend(gd->unsortable_sets, gs);

                // Validate that unsortable set is hashable
                if (bms_overlap_list(gd->unhashable_refs, gset))
                    ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                             errmsg("could not implement GROUP BY"),
                             errdetail("Some of the datatypes only support hashing, while others only support sorting.")));
            } else {
                sortable_sets = lappend(sortable_sets, gset);
            }
        }

        sets = sortable_sets ? extract_rollup_sets(sortable_sets) : NIL;
    } else {
        sets = extract_rollup_sets(parse->groupingSets);
    }

    // Process rollup sets
    foreach(lc_set, sets) {
        List *current_sets = (List *) lfirst(lc_set);
        RollupData *rollup = makeNode(RollupData);

        // Reorder sets optimally (match ORDER BY if single pass)
        current_sets = reorder_grouping_sets(current_sets,
                                              (list_length(sets) == 1
                                               ? parse->sortClause : NIL));

        GroupingSetData *gs = linitial_node(GroupingSetData, current_sets);

        // Set up group clause for this rollup
        if (gs->set)
            rollup->groupClause = preprocess_groupclause(root, gs->set);
        else
            rollup->groupClause = NIL;

        // Check if hashable
        if (gs->set && !bms_overlap_list(gd->unhashable_refs, gs->set)) {
            rollup->hashable = true;
            gd->any_hashable = true;
        }

        // Remap to column indices
        rollup->gsets = remap_to_groupclause_idx(rollup->groupClause,
                                                 current_sets,
                                                 gd->tleref_to_colnum_map);
        rollup->gsets_data = current_sets;

        gd->rollups = lappend(gd->rollups, rollup);
    }

    // Handle unsortable sets index mapping
    if (gd->unsortable_sets) {
        gd->hash_sets_idx = remap_to_groupclause_idx(parse->groupClause,
                                                     gd->unsortable_sets,
                                                     gd->tleref_to_colnum_map);
        gd->any_hashable = true;
    }

    return gd;
}
```