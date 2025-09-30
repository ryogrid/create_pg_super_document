# set_cte_pathlist

## Location
[src/backend/optimizer/path/allpaths.c:2860-2938](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/allpaths.c#L2860-L2938)

## Overview
Builds the single access path for a non-self-reference CTE RTE (Range Table Entry), handling pathlist generation for Common Table Expression scans in PostgreSQL's query planner.

## Definition
```c
static void set_cte_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
```

## Detailed Description
This function is responsible for creating access paths for non-self-referencing Common Table Expression (CTE) RTEs in PostgreSQL's query optimizer. CTEs are WITH clauses that define temporary named result sets that can be referenced in the main query. This function handles the complex task of locating the previously planned CTE, extracting its path and plan information, and creating an appropriate scan path for accessing the CTE's results.

The function navigates up the planner hierarchy to find the CTE's definition and corresponding plan, converts pathkeys to the outer query's representation, and handles size estimates. Unlike self-referencing CTEs (recursive CTEs), this handles the simpler case where a CTE is referenced but doesn't reference itself.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global information about the query being planned
- `rel`: RelOptInfo structure representing the relation (CTE) for which paths are being generated
- `rte`: RangeTblEntry representing the CTE reference in the query's range table

## Dependencies
- Functions called/Symbols referenced:
  - CommonTableExpr
  - [list_nth_int](../l/list_nth_int.md)
  - [list_nth](../l/list_nth.md)
  - [set_cte_size_estimates](set_cte_size_estimates.md)
  - [convert_subquery_pathkeys](../c/convert_subquery_pathkeys.md)
  - [add_path](../a/add_path.md)
  - [create_ctescan_path](../c/create_ctescan_path.md)
- Called from (representative examples):
  - [set_rel_size](set_rel_size.md)

## Notes and Other Information
- Does not support join-qual-parameterized paths for CTEs, eliminating the need for a separate set_cte_size phase
- CTE scans do not support pushing join clauses into their quals, but can have required parameterization due to LATERAL references in their target lists
- The function walks up the planner hierarchy using ctelevelsup to find the appropriate CTE definition and plan
- Converts pathkeys from the CTE's context to the outer query's representation using convert_subquery_pathkeys
- Uses plan_id to locate the corresponding path and plan from the global subpaths and subplans lists
- Includes extensive error checking to ensure the referenced CTE exists and has been properly planned
- Located in src/backend/optimizer/path/allpaths.c:2860-2938

## Simplified Source

```c
static void
set_cte_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
    Path *ctepath;
    Plan *cteplan;
    PlannerInfo *cteroot;
    Index levelsup;
    List *pathkeys;
    int ndx;
    ListCell *lc;
    int plan_id;
    Relids required_outer;

    // Find the referenced CTE by walking up the planner hierarchy
    levelsup = rte->ctelevelsup;
    cteroot = root;
    while (levelsup-- > 0)
    {
        cteroot = cteroot->parent_root;
        if (!cteroot)
            elog(ERROR, "bad levelsup for CTE \"%s\"", rte->ctename);
    }

    // Search for the CTE by name in the CTE list
    ndx = 0;
    foreach(lc, cteroot->parse->cteList)
    {
        CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc);
        if (strcmp(cte->ctename, rte->ctename) == 0)
            break;
        ndx++;
    }

    // Verify CTE was found and has a valid plan
    if (lc == NULL)
        elog(ERROR, "could not find CTE \"%s\"", rte->ctename);
    if (ndx >= list_length(cteroot->cte_plan_ids))
        elog(ERROR, "could not find plan for CTE \"%s\"", rte->ctename);

    plan_id = list_nth_int(cteroot->cte_plan_ids, ndx);
    if (plan_id <= 0)
        elog(ERROR, "no plan was made for CTE \"%s\"", rte->ctename);

    // Retrieve the CTE's path and plan from global lists
    ctepath = (Path *) list_nth(root->glob->subpaths, plan_id - 1);
    cteplan = (Plan *) list_nth(root->glob->subplans, plan_id - 1);

    // Set size estimates for the relation
    set_cte_size_estimates(root, rel, cteplan->plan_rows);

    // Convert CTE pathkeys to outer query representation
    pathkeys = convert_subquery_pathkeys(root, rel, ctepath->pathkeys, cteplan->targetlist);

    // Handle LATERAL references as required parameterization
    required_outer = rel->lateral_relids;

    // Create and add the CTE scan path
    add_path(rel, create_ctescan_path(root, rel, pathkeys, required_outer));
}
```