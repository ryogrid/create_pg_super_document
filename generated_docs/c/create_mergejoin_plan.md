# create_mergejoin_plan

## Location
[src/backend/optimizer/plan/createplan.c:4440-4746](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L4440-L4746)

## Overview
Creates a MergeJoin plan node from a MergePath, implementing merge joins where two pre-sorted input relations are merged together based on equality conditions.

## Definition

```c
struction (see find_mergeclauses_for_outer_pathkeys()). There
		 * could be more than one mergeclause for the same outer pathkey, but
		 * no pathkey may be entirely skipped over.
		 */
		if (oeclass != opeclass)	/* multiple matches are not interesting */
		{
			/* doesn't match the current opathkey, so must match the next */
			if (lop == NULL)
				elog(ERROR, "outer pathkeys do not match mergeclauses");
			opathkey = (PathKey *) lfirst(lop);
			opeclass = opathkey->pk_eclass;
			lop = lnext(outerpathkeys, lop);
			if (oeclass != opeclass)
				elog(ERROR, "outer pathkeys do not match mergeclauses");
		}

		/*
		 * The inner pathkeys likewise should not have skipped-over keys, but
		 * it's possible for a mergeclause to reference some earlier inner
		 * pathkey if we had redundant pathkeys.  For example we might have
		 * mergeclauses like "o.a = i.x AND o.b = i.y AND o.c = i.x".  The
		 * implied inner ordering is then "ORDER BY x, y, x", but the pathkey
		 * mechanism drops the second sort by x as redundant, and this code
		 * must cope.
		 *
		 * It's also possible for the implied inner-rel ordering to be like
		 * "ORDER BY x, y, x DESC".  We still drop the second instance of x as
		 * redundant;
```
## Detailed Description
This function creates a MergeJoin execution plan node from a MergePath. Merge joins are efficient when both input relations are already sorted (or can be cheaply sorted) on the join columns. The function handles complex pathkey matching between outer and inner relations, creates explicit Sort nodes when necessary, and sets up the merge operation arrays needed by the executor. It also handles materialize nodes for the inner relation when mark/restore operations are needed, processes join clauses appropriately for different join types, and manages redundant pathkeys and sort ordering requirements.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning context and state information
- : MergePath representing the chosen merge join access path with sort requirements and merge clauses

## Dependencies
- Functions called/Symbols referenced:
  - [build_path_tlist](../b/build_path_tlist.md)
  - [create_plan_recurse](create_plan_recurse.md)
  - [order_qual_clauses](../o/order_qual_clauses.md)
  - IS_OUTER_JOIN
  - [extract_actual_join_clauses](../e/extract_actual_join_clauses.md)
  - [extract_actual_clauses](../e/extract_actual_clauses.md)
  - [get_actual_clauses](../g/get_actual_clauses.md)
  - [list_difference](../l/list_difference.md)
  - [replace_nestloop_params](../r/replace_nestloop_params.md)
  - [get_switched_clauses](../g/get_switched_clauses.md)
  - [make_sort_from_pathkeys](../m/make_sort_from_pathkeys.md)
  - [label_sort_with_costsize](../l/label_sort_with_costsize.md)
  - [make_material](../m/make_material.md)
  - [copy_plan_costsize](copy_plan_costsize.md)
  - [make_mergejoin](../m/make_mergejoin.md)
  - [copy_generic_path_info](copy_generic_path_info.md)
- Called from (representative examples):
  - [create_join_plan](create_join_plan.md)

## Notes and Other Information
- Merge joins are most efficient when input relations are already sorted on join columns
- Automatically creates Sort nodes for inputs that require sorting
- Handles complex pathkey matching including redundant and partially overlapping sort orders
- May add a Material node to the inner plan to support mark/restore operations for outer joins
- Sets up arrays of merge families, collations, strategies, and null-handling flags for the executor
- Manages both simple equality joins and complex multi-column merge conditions
- The pathkey matching logic handles cases where merge clauses may reference the same pathkey multiple times
- Located at src/backend/optimizer/plan/createplan.c:4440-4746
- Part of the JOIN METHODS section of the planner

## Simplified Source

```c
static MergeJoin *create_mergejoin_plan(PlannerInfo *root, MergePath *best_path) {
    List *tlist = build_path_tlist(root, &best_path->jpath.path);

    // Create input plans, requesting small tlists if sorting needed
    Plan *outer_plan = create_plan_recurse(root, best_path->jpath.outerjoinpath,
                                          (best_path->outersortkeys != NIL) ? CP_SMALL_TLIST : 0);
    Plan *inner_plan = create_plan_recurse(root, best_path->jpath.innerjoinpath,
                                          (best_path->innersortkeys != NIL) ? CP_SMALL_TLIST : 0);

    // Process join clauses
    List *joinclauses = order_qual_clauses(root, best_path->jpath.joinrestrictinfo);
    List *otherclauses = NIL;

    if (IS_OUTER_JOIN(best_path->jpath.jointype)) {
        extract_actual_join_clauses(joinclauses, best_path->jpath.path.parent->relids,
                                   &joinclauses, &otherclauses);
    } else {
        joinclauses = extract_actual_clauses(joinclauses, false);
    }

    // Extract merge clauses and remove from join clauses
    List *mergeclauses = get_actual_clauses(best_path->path_mergeclauses);
    joinclauses = list_difference(joinclauses, mergeclauses);

    // Handle nested loop parameters if needed
    if (best_path->jpath.path.param_info) {
        joinclauses = (List *) replace_nestloop_params(root, (Node *) joinclauses);
        otherclauses = (List *) replace_nestloop_params(root, (Node *) otherclauses);
    }

    // Arrange merge clauses with outer variable on left
    mergeclauses = get_switched_clauses(best_path->path_mergeclauses,
                                       best_path->jpath.outerjoinpath->parent->relids);

    // Create sort nodes if needed for outer plan
    List *outerpathkeys;
    if (best_path->outersortkeys) {
        Sort *sort = make_sort_from_pathkeys(outer_plan, best_path->outersortkeys,
                                           best_path->jpath.outerjoinpath->parent->relids);
        label_sort_with_costsize(root, sort, -1.0);
        outer_plan = (Plan *) sort;
        outerpathkeys = best_path->outersortkeys;
    } else {
        outerpathkeys = best_path->jpath.outerjoinpath->pathkeys;
    }

    // Create sort nodes if needed for inner plan
    List *innerpathkeys;
    if (best_path->innersortkeys) {
        Sort *sort = make_sort_from_pathkeys(inner_plan, best_path->innersortkeys,
                                           best_path->jpath.innerjoinpath->parent->relids);
        label_sort_with_costsize(root, sort, -1.0);
        inner_plan = (Plan *) sort;
        innerpathkeys = best_path->innersortkeys;
    } else {
        innerpathkeys = best_path->jpath.innerjoinpath->pathkeys;
    }

    // Add materialize node if needed for mark/restore support
    if (best_path->materialize_inner) {
        Plan *matplan = (Plan *) make_material(inner_plan);
        copy_plan_costsize(matplan, inner_plan);
        matplan->total_cost += cpu_operator_cost * matplan->plan_rows;
        inner_plan = matplan;
    }

    // Build merge operation arrays for executor
    int nClauses = list_length(mergeclauses);
    Oid *mergefamilies = palloc(nClauses * sizeof(Oid));
    Oid *mergecollations = palloc(nClauses * sizeof(Oid));
    int *mergestrategies = palloc(nClauses * sizeof(int));
    bool *mergenullsfirst = palloc(nClauses * sizeof(bool));

    // Match pathkeys with merge clauses to extract sort information
    PathKey *opathkey = NULL;
    EquivalenceClass *opeclass = NULL;
    ListCell *lop = list_head(outerpathkeys);
    ListCell *lip = list_head(innerpathkeys);

    int i = 0;
    foreach(cell, best_path->path_mergeclauses) {
        RestrictInfo *rinfo = lfirst_node(RestrictInfo, cell);

        // Extract equivalence classes from merge clause
        EquivalenceClass *oeclass = rinfo->outer_is_left ? rinfo->left_ec : rinfo->right_ec;
        EquivalenceClass *ieclass = rinfo->outer_is_left ? rinfo->right_ec : rinfo->left_ec;

        // Match outer pathkey
        if (oeclass != opeclass) {
            if (lop == NULL)
                elog(ERROR, "outer pathkeys do not match mergeclauses");
            opathkey = (PathKey *) lfirst(lop);
            opeclass = opathkey->pk_eclass;
            lop = lnext(outerpathkeys, lop);
            if (oeclass != opeclass)
                elog(ERROR, "outer pathkeys do not match mergeclauses");
        }

        // Match inner pathkey (handling redundant pathkeys)
        PathKey *ipathkey = NULL;
        bool first_inner_match = false;

        if (lip) {
            ipathkey = (PathKey *) lfirst(lip);
            if (ieclass == ipathkey->pk_eclass) {
                lip = lnext(innerpathkeys, lip);
                first_inner_match = true;
            }
        }

        if (!first_inner_match) {
            // Find matching pathkey in earlier positions
            foreach(cell2, innerpathkeys) {
                if (cell2 == lip) break;
                ipathkey = (PathKey *) lfirst(cell2);
                if (ieclass == ipathkey->pk_eclass) break;
            }
            if (ieclass != ipathkey->pk_eclass)
                elog(ERROR, "inner pathkeys do not match mergeclauses");
        }

        // Extract merge operation info
        mergefamilies[i] = opathkey->pk_opfamily;
        mergecollations[i] = opathkey->pk_eclass->ec_collation;
        mergestrategies[i] = opathkey->pk_strategy;
        mergenullsfirst[i] = opathkey->pk_nulls_first;
        i++;
    }

    // Create MergeJoin node
    MergeJoin *join_plan = make_mergejoin(tlist, joinclauses, otherclauses, mergeclauses,
                                         mergefamilies, mergecollations, mergestrategies,
                                         mergenullsfirst, outer_plan, inner_plan,
                                         best_path->jpath.jointype, best_path->jpath.inner_unique,
                                         best_path->skip_mark_restore);

    copy_generic_path_info(&join_plan->join.plan, &best_path->jpath.path);
    return join_plan;
}
```