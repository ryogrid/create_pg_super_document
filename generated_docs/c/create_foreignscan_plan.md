# create_foreignscan_plan

## Location
[src/backend/optimizer/plan/createplan.c:4122-4276](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L4122-L4276)

## Overview
Creates a ForeignScan plan node for scanning a relation using a Foreign Data Wrapper (FDW), handling both base relations and foreign joins with appropriate optimization and parameter handling.

## Definition

```c
structures, so compute it here.
	 */
	scan_plan->fs_base_relids = bms_difference(scan_plan->fs_relids,
											   root->outer_join_rels);
```
## Detailed Description
This function is responsible for creating a ForeignScan execution plan node from a ForeignPath. It coordinates with the Foreign Data Wrapper (FDW) to generate an optimized plan for accessing foreign data. The function handles multiple scenarios including base foreign table scans, foreign joins, and parameterized foreign scans. It ensures proper cost propagation, handles outer plan creation for complex foreign operations, and manages system column detection for base relations. The function also performs nestloop parameter replacement for parameterized scans and sets up relid information needed for proper execution planning.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning context and state information
- : ForeignPath representing the chosen access path for the foreign relation
- : Target list specifying which columns/expressions should be returned by the scan
- : List of restriction clauses to be applied during the scan

## Dependencies
- Functions called/Symbols referenced:
  - [create_plan_recurse](create_plan_recurse.md)
  - planner_rt_fetch
  - [order_qual_clauses](../o/order_qual_clauses.md)
  - [copy_generic_path_info](copy_generic_path_info.md)
  - [bms_difference](../b/bms_difference.md)
  - [replace_nestloop_params](../r/replace_nestloop_params.md)
  - [pull_varattnos](../p/pull_varattnos.md)
  - [bms_is_member](../b/bms_is_member.md)
  - [bms_free](../b/bms_free.md)
- Called from (representative examples):
  - [create_scan_plan](create_scan_plan.md)

## Notes and Other Information
- The function delegates actual plan generation to the FDW's GetForeignPlan callback, allowing FDWs to customize their execution strategy
- Handles both simple foreign table scans and complex foreign joins
- Automatically detects and flags when system columns are requested from base relations
- Performs parameter replacement for nested loop joins involving foreign scans
- Sets up relid tracking for proper join planning and execution
- Manages user access permissions and role dependencies for foreign joins
- Located at src/backend/optimizer/plan/createplan.c:4122-4276

## Simplified Source

```c
static ForeignScan *
create_foreignscan_plan(PlannerInfo *root, ForeignPath *best_path,
                       List *tlist, List *scan_clauses) {
    RelOptInfo *rel = best_path->path.parent;
    Index scan_relid = rel->relid;
    Oid rel_oid = InvalidOid;
    Plan *outer_plan = NULL;

    Assert(rel->fdwroutine != NULL);

    // Handle child path for complex foreign operations
    if (best_path->fdw_outerpath)
        outer_plan = create_plan_recurse(root, best_path->fdw_outerpath, CP_EXACT_TLIST);

    // Get relation OID for base relations
    if (scan_relid > 0) {
        RangeTblEntry *rte = planner_rt_fetch(scan_relid, root);
        Assert(rte->rtekind == RTE_RELATION);
        rel_oid = rte->relid;
    }

    // Optimize scan clause order
    scan_clauses = order_qual_clauses(root, scan_clauses);

    // Delegate to FDW to create the actual plan
    ForeignScan *scan_plan = rel->fdwroutine->GetForeignPlan(root, rel, rel_oid,
                                                            best_path, tlist,
                                                            scan_clauses, outer_plan);

    // Copy standard path information
    copy_generic_path_info(&scan_plan->scan.plan, &best_path->path);

    // Set access permissions and server information
    scan_plan->checkAsUser = rel->userid;
    scan_plan->fs_server = rel->serverid;

    // Set relation IDs represented by this scan
    if (rel->reloptkind == RELOPT_UPPER_REL)
        scan_plan->fs_relids = root->all_query_rels;
    else
        scan_plan->fs_relids = best_path->path.parent->relids;

    // Compute base relation IDs (excluding outer joins)
    scan_plan->fs_base_relids = bms_difference(scan_plan->fs_relids, root->outer_join_rels);

    // Mark role dependency for foreign joins
    if (rel->useridiscurrent)
        root->glob->dependsOnRole = true;

    // Handle nestloop parameter replacement
    if (best_path->path.param_info) {
        scan_plan->scan.plan.qual = (List *)
            replace_nestloop_params(root, (Node *) scan_plan->scan.plan.qual);
        scan_plan->fdw_exprs = (List *)
            replace_nestloop_params(root, (Node *) scan_plan->fdw_exprs);
        scan_plan->fdw_recheck_quals = (List *)
            replace_nestloop_params(root, (Node *) scan_plan->fdw_recheck_quals);
    }

    // Check for system column usage in base relations
    scan_plan->fsSystemCol = false;
    if (scan_relid > 0) {
        Bitmapset *attrs_used = NULL;

        // Collect all referenced attributes
        pull_varattnos((Node *) rel->reltarget->exprs, scan_relid, &attrs_used);
        foreach(lc, rel->baserestrictinfo) {
            RestrictInfo *rinfo = lfirst(lc);
            pull_varattnos((Node *) rinfo->clause, scan_relid, &attrs_used);
        }

        // Check for system columns
        for (int i = FirstLowInvalidHeapAttributeNumber + 1; i < 0; i++) {
            if (bms_is_member(i - FirstLowInvalidHeapAttributeNumber, attrs_used)) {
                scan_plan->fsSystemCol = true;
                break;
            }
        }
        bms_free(attrs_used);
    }

    return scan_plan;
}
```