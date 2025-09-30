# set_subquery_pathlist

## Location
[src/backend/optimizer/path/allpaths.c:2482-2748](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/allpaths.c#L2482-L2748)

## Overview
Generates SubqueryScan access paths for a subquery RTE by planning the subquery and creating corresponding outer query paths.

## Definition
```c
static void set_subquery_pathlist(PlannerInfo *root, RelOptInfo *rel,
                                  Index rti, RangeTblEntry *rte)
```

## Detailed Description
This function is responsible for creating access paths for subqueries in the PostgreSQL query planner. It performs several key optimizations: (1) attempts to push down WHERE clauses from the outer query into the subquery to improve subquery planning, (2) tries to create window function run conditions for early termination, (3) removes unused output columns from the subquery, (4) plans the subquery using subquery_planner(), and (5) creates SubqueryScan paths in the outer query for each path produced by the subquery planner. The function handles both regular and parallel paths, and includes special logic for LATERAL subqueries and security barrier views. It also determines tuple_fraction hints to pass to the subquery planner based on the outer query's characteristics.

## Parameters / Member Variables
- `root`: PlannerInfo for the current query level
- `rel`: RelOptInfo for the subquery relation being planned
- `rti`: Range table index of the subquery RTE
- `rte`: RangeTblEntry containing the subquery to be planned

## Dependencies
- Functions called/Symbols referenced:
  - copyObject (deep copy the subquery)
  - memset, palloc0 (memory management)
  - [subquery_is_pushdown_safe](subquery_is_pushdown_safe.md) (check if clauses can be pushed down)
  - [qual_is_pushdown_safe](../q/qual_is_pushdown_safe.md) (check individual clause safety)
  - [subquery_push_qual](subquery_push_qual.md) (push clause into subquery)
  - [check_and_push_window_quals](../c/check_and_push_window_quals.md) (attempt window run conditions)
  - [remove_unused_subquery_outputs](../r/remove_unused_subquery_outputs.md) (optimize subquery output)
  - [subquery_planner](subquery_planner.md) (plan the subquery)
  - [fetch_upper_rel](../f/fetch_upper_rel.md) (get final relation from subquery)
  - [set_dummy_rel_pathlist](set_dummy_rel_pathlist.md) (handle empty subqueries)
  - [set_subquery_size_estimates](set_subquery_size_estimates.md) (set size estimates)
  - [convert_subquery_pathkeys](../c/convert_subquery_pathkeys.md) (convert pathkeys to outer context)
  - [create_subqueryscan_path](../c/create_subqueryscan_path.md) (create SubqueryScan paths)
  - [add_path](../a/add_path.md), add_partial_path (add paths to relation)
- Called from (representative examples):
  - pushdown_safe_type
  - [set_rel_size](set_rel_size.md)

## Notes and Other Information
- This is a static function accessible only within allpaths.c
- Does not currently support parameterized paths by pushing join clauses into subqueries
- Handles LATERAL subqueries by setting required_outer appropriately
- Respects security_barrier flag to prevent leaky function pushdown in views with RLS
- Uses pushdown_safety_info structure to track reasons why columns are unsafe for pushdown
- Determines whether to pass tuple_fraction hint based on outer query complexity
- Creates both regular and parallel SubqueryScan paths when appropriate
- Optimizes for trivial pathtargets (direct column references in order)
- Located in src/backend/optimizer/path/allpaths.c at lines 2482-2748
- Central to subquery optimization and one of the more complex functions in the path generation system

## Simplified Source

```c
static void
set_subquery_pathlist(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte)
{
    Query *subquery = rte->subquery;
    bool trivial_pathtarget;
    Relids required_outer;
    pushdown_safety_info safetyInfo;
    double tuple_fraction;
    RelOptInfo *sub_final_rel;
    ListCell *lc;

    // Copy subquery to avoid modifying the original
    subquery = copyObject(subquery);

    // Handle LATERAL parameterization
    required_outer = rel->lateral_relids;

    // Initialize safety tracking for qual pushdown
    memset(&safetyInfo, 0, sizeof(safetyInfo));
    safetyInfo.unsafeFlags = (unsigned char *)
        palloc0((list_length(subquery->targetList) + 1) * sizeof(unsigned char));
    safetyInfo.unsafeLeaky = rte->security_barrier;

    // Try to push down restriction clauses into the subquery
    if (rel->baserestrictinfo != NIL &&
        subquery_is_pushdown_safe(subquery, subquery, &safetyInfo))
    {
        List *upperrestrictlist = NIL;

        foreach(l, rel->baserestrictinfo)
        {
            RestrictInfo *rinfo = (RestrictInfo *) lfirst(l);
            Node *clause = (Node *) rinfo->clause;

            if (rinfo->pseudoconstant)
            {
                upperrestrictlist = lappend(upperrestrictlist, rinfo);
                continue;
            }

            switch (qual_is_pushdown_safe(subquery, rti, rinfo, &safetyInfo))
            {
                case PUSHDOWN_SAFE:
                    // Push the clause down into subquery
                    subquery_push_qual(subquery, rte, rti, clause);
                    break;

                case PUSHDOWN_WINDOWCLAUSE_RUNCOND:
                    // Try to use as window run condition, otherwise keep in upper query
                    if (!subquery->hasWindowFuncs ||
                        check_and_push_window_quals(subquery, rte, rti, clause, &run_cond_attrs))
                    {
                        upperrestrictlist = lappend(upperrestrictlist, rinfo);
                    }
                    break;

                case PUSHDOWN_UNSAFE:
                    upperrestrictlist = lappend(upperrestrictlist, rinfo);
                    break;
            }
        }
        rel->baserestrictinfo = upperrestrictlist;
    }

    pfree(safetyInfo.unsafeFlags);

    // Remove unused output columns from subquery
    remove_unused_subquery_outputs(subquery, rel, run_cond_attrs);

    // Determine tuple fraction hint for subquery planning
    if (parse->hasAggs || parse->groupClause || parse->groupingSets ||
        root->hasHavingQual || parse->distinctClause || parse->sortClause ||
        bms_membership(root->all_baserels) == BMS_MULTIPLE)
        tuple_fraction = 0.0;  // Complex outer query - plan for full retrieval
    else
        tuple_fraction = root->tuple_fraction;  // Simple outer query - pass hint down

    // Plan the subquery
    rel->subroot = subquery_planner(root->glob, subquery, root, false, tuple_fraction, NULL);
    rel->subplan_params = root->plan_params;
    root->plan_params = NIL;

    // Check if subquery was proven empty by constraint exclusion
    sub_final_rel = fetch_upper_rel(rel->subroot, UPPERREL_FINAL, NULL);
    if (IS_DUMMY_REL(sub_final_rel))
    {
        set_dummy_rel_pathlist(rel);
        return;
    }

    // Set size estimates for the subquery relation
    set_subquery_size_estimates(root, rel);

    // Check if reltarget is trivial (fetches all columns in order)
    trivial_pathtarget = (list_length(rel->reltarget->exprs) == list_length(subquery->targetList));
    if (trivial_pathtarget)
    {
        foreach(lc, rel->reltarget->exprs)
        {
            Node *node = (Node *) lfirst(lc);
            if (!IsA(node, Var) ||
                ((Var *) node)->varno != rti ||
                ((Var *) node)->varattno != foreach_current_index(lc) + 1)
            {
                trivial_pathtarget = false;
                break;
            }
        }
    }

    // Create SubqueryScan paths for each subquery path
    foreach(lc, sub_final_rel->pathlist)
    {
        Path *subpath = (Path *) lfirst(lc);
        List *pathkeys;

        // Convert subquery pathkeys to outer query representation
        pathkeys = convert_subquery_pathkeys(root, rel, subpath->pathkeys,
                                             make_tlist_from_pathtarget(subpath->pathtarget));

        // Create and add the SubqueryScan path
        add_path(rel, (Path *)
                 create_subqueryscan_path(root, rel, subpath, trivial_pathtarget,
                                          pathkeys, required_outer));
    }

    // Handle parallel paths if appropriate
    if (rel->consider_parallel && bms_is_empty(required_outer))
    {
        foreach(lc, sub_final_rel->partial_pathlist)
        {
            Path *subpath = (Path *) lfirst(lc);
            List *pathkeys;

            pathkeys = convert_subquery_pathkeys(root, rel, subpath->pathkeys,
                                                 make_tlist_from_pathtarget(subpath->pathtarget));

            add_partial_path(rel, (Path *)
                             create_subqueryscan_path(root, rel, subpath, trivial_pathtarget,
                                                      pathkeys, required_outer));
        }
    }
}
```