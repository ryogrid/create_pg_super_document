# SS_process_ctes

## Location
[src/backend/optimizer/plan/subselect.c:880-1055](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/subselect.c#L880-L1055)

## Overview
Processes a query's WITH list by determining whether to ignore, inline, or convert each Common Table Expression (CTE) to an initplan based on usage patterns and characteristics.

## Definition
```c
void SS_process_ctes(PlannerInfo *root)
```

## Detailed Description
This function is responsible for handling all Common Table Expressions (CTEs) in a query's WITH clause. For each CTE, it makes strategic decisions about execution:

1. **Ignoring**: Unreferenced SELECT CTEs are ignored as they produce no useful output.
2. **Inlining**: CTEs that meet specific criteria are converted to regular sub-SELECT-in-FROM constructs, allowing better optimization integration.
3. **Initplan Conversion**: CTEs that cannot be inlined are converted to initplans with proper parameter management.

The inlining decision considers multiple factors:
- User preferences (CTEMaterializeAlways/Never flags)
- Reference count (single vs. multiple references)  
- Recursiveness
- Side-effects (non-SELECT commands, volatile functions)
- Self-references to recursive CTEs

For non-inlined CTEs, the function creates SubPlan nodes, manages parameter assignments for communication between CTE scans, and integrates the plans into the global subplan infrastructure.

## Parameters
- `root`: PlannerInfo containing query context and CTE information

## Dependencies
- Functions called/Symbols referenced:
  - [contain_dml](../c/contain_dml.md)
  - [contain_outer_selfref](../c/contain_outer_selfref.md)
  - [contain_volatile_functions](../c/contain_volatile_functions.md)
  - [inline_cte](../i/inline_cte.md)
  - copyObject
  - [subquery_planner](../s/subquery_planner.md)
  - [fetch_upper_rel](../f/fetch_upper_rel.md)
  - [create_plan](../c/create_plan.md)
  - [get_first_col_type](../g/get_first_col_type.md)
  - [assign_special_exec_param](../a/assign_special_exec_param.md)
  - [cost_subplan](../c/cost_subplan.md)
  - makeNode
  - [lappend](../l/lappend.md)
  - [lappend_int](../l/lappend_int.md)
  - list_make1_int
  - [psprintf](../p/psprintf.md)
- Called from (representative examples):
  - [subquery_planner](../s/subquery_planner.md)

## Notes and Other Information
- Fills in root->cte_plan_ids with parallel list to root->parse->cteList containing subplan IDs or -1 for inlined/ignored CTEs
- CTE scans are not considered for parallelism due to potential side-effects
- Parameter management uses special execution parameters for communication between CteScan nodes
- Inlining decisions balance duplicate computation costs against optimization opportunities
- Error checking ensures CTEs don't request parameters from outer query levels inappropriately

## Simplified Source

```c
void SS_process_ctes(PlannerInfo *root) {
    foreach(lc, root->parse->cteList) {
        CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc);
        CmdType cmdType = ((Query *) cte->ctequery)->commandType;

        // Skip unreferenced SELECT CTEs
        if (cte->cterefcount == 0 && cmdType == CMD_SELECT) {
            root->cte_plan_ids = lappend_int(root->cte_plan_ids, -1);
            continue;
        }

        // Determine if we can inline this CTE
        bool can_inline =
            (cte->ctematerialized == CTEMaterializeNever ||
             (cte->ctematerialized == CTEMaterializeDefault && cte->cterefcount == 1)) &&
            !cte->cterecursive &&
            cmdType == CMD_SELECT &&
            !contain_dml(cte->ctequery) &&
            (cte->cterefcount <= 1 || !contain_outer_selfref(cte->ctequery)) &&
            !contain_volatile_functions(cte->ctequery);

        if (can_inline) {
            // Inline the CTE as a regular sub-SELECT
            inline_cte(root, cte);
            root->cte_plan_ids = lappend_int(root->cte_plan_ids, -1);
            continue;
        }

        // Convert CTE to an initplan
        Query *subquery = (Query *) copyObject(cte->ctequery);

        // Plan the CTE subquery
        PlannerInfo *subroot = subquery_planner(root->glob, subquery, root,
                                               cte->cterecursive, 0.0, NULL);

        // Create the execution plan
        RelOptInfo *final_rel = fetch_upper_rel(subroot, UPPERREL_FINAL, NULL);
        Path *best_path = final_rel->cheapest_total_path;
        Plan *plan = create_plan(subroot, best_path);

        // Create SubPlan node for the CTE
        SubPlan *splan = makeNode(SubPlan);
        splan->subLinkType = CTE_SUBLINK;
        splan->parallel_safe = false;  // CTEs not considered for parallelism
        get_first_col_type(plan, &splan->firstColType, &splan->firstColTypmod,
                          &splan->firstColCollation);

        // Assign parameter ID for CTE output communication
        int paramid = assign_special_exec_param(root);
        splan->setParam = list_make1_int(paramid);

        // Add to global subplan lists
        root->glob->subplans = lappend(root->glob->subplans, plan);
        root->glob->subpaths = lappend(root->glob->subpaths, best_path);
        root->glob->subroots = lappend(root->glob->subroots, subroot);
        splan->plan_id = list_length(root->glob->subplans);

        // Add to initplans and track plan ID
        root->init_plans = lappend(root->init_plans, splan);
        root->cte_plan_ids = lappend_int(root->cte_plan_ids, splan->plan_id);

        // Set labels and costs
        splan->plan_name = psprintf("CTE %s", cte->ctename);
        cost_subplan(root, splan, plan);
    }
}
```