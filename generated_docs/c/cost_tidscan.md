# cost_tidscan

## Location
[src/backend/optimizer/path/costsize.c:1249-1356](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L1249-L1356)

## Overview
Determines and returns the cost of scanning a relation using TIDs (tuple identifiers), calculating both startup and per-tuple costs for TID-based access paths.

## Definition

```c
void
cost_tidscan(Path *path, PlannerInfo *root,
			 RelOptInfo *baserel, List *tidquals, ParamPathInfo *param_info)
```
## Detailed Description
The  function calculates the cost of performing a TID scan on a relation, which is a direct access method that uses tuple identifiers to locate specific rows. This function handles several scenarios including regular TID equality comparisons, TID array operations (ScalarArrayOpExpr), and CURRENT OF expressions used in cursors. The costing model accounts for the fact that each TID typically corresponds to a different page, so random page access costs are applied. Special handling is provided for CURRENT OF expressions, which are forced to use TID scans and have their disable costs subtracted to prevent other scan types from being chosen.

## Parameters / Member Variables
- : Output parameter where the calculated costs will be stored
- : PlannerInfo structure containing global planner state
- : RelOptInfo for the relation being scanned
- : List of TID-checkable qualification clauses
- : ParamPathInfo for parameterized paths, or NULL for non-parameterized paths

## Dependencies
- Functions called/Symbols referenced:
  - [estimate_array_length](../e/estimate_array_length.md)
  - [cost_qual_eval](cost_qual_eval.md)
  - [get_tablespace_page_costs](../g/get_tablespace_page_costs.md)
  - [get_restriction_qual_cost](../g/get_restriction_qual_cost.md)
  - lsecond
- Called from (representative examples):
  - [create_tidscan_path](create_tidscan_path.md)

## Notes and Other Information
- Only applies to base relations (not joins or subqueries)
- Each TID is assumed to be on a different page, leading to random I/O costs
- CURRENT OF expressions receive special treatment to force TID scan usage
- The enable_tidscan GUC parameter is honored except when CURRENT OF is present
- TID quals are assumed to be a subset of the overall restriction quals
- Array-based TID operations are supported through ScalarArrayOpExpr handling

## Simplified Source

```c
void
cost_tidscan(Path *path, PlannerInfo *root, RelOptInfo *baserel,
             List *tidquals, ParamPathInfo *param_info)
{
    Cost startup_cost = 0, run_cost = 0;
    bool isCurrentOf = false;
    QualCost qpqual_cost, tid_qual_cost;
    Cost cpu_per_tuple;
    double ntuples = 0;
    double spc_random_page_cost;
    ListCell *l;

    Assert(baserel->relid > 0 && baserel->rtekind == RTE_RELATION);

    // Set row estimate based on parameterization
    if (param_info)
        path->rows = param_info->ppi_rows;
    else
        path->rows = baserel->rows;

    // Count expected tuples based on TID qualification types
    foreach(l, tidquals) {
        RestrictInfo *rinfo = lfirst_node(RestrictInfo, l);
        Expr *qual = rinfo->clause;

        if (IsA(qual, ScalarArrayOpExpr)) {
            // Array of TIDs - each element yields 1 tuple
            ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *) qual;
            Node *arraynode = (Node *) lsecond(saop->args);
            ntuples += estimate_array_length(root, arraynode);
        }
        else if (IsA(qual, CurrentOfExpr)) {
            // CURRENT OF yields exactly 1 tuple
            isCurrentOf = true;
            ntuples++;
        }
        else {
            // Simple CTID = constant yields 1 tuple
            ntuples++;
        }
    }

    // Handle enable_tidscan and CURRENT OF special cases
    if (isCurrentOf) {
        // CURRENT OF forces TID scan, subtract disable cost
        Assert(baserel->baserestrictcost.startup >= disable_cost);
        startup_cost -= disable_cost;
    }
    else if (!enable_tidscan) {
        startup_cost += disable_cost;
    }

    // Calculate TID qualification costs
    cost_qual_eval(&tid_qual_cost, tidquals, root);

    // Get tablespace page costs (only need random cost)
    get_tablespace_page_costs(baserel->reltablespace, &spc_random_page_cost, NULL);

    // I/O costs - assume each TID is on different page (random access)
    run_cost += spc_random_page_cost * ntuples;

    // CPU costs for scanning and qualification
    get_restriction_qual_cost(root, baserel, param_info, &qpqual_cost);

    startup_cost += qpqual_cost.startup + tid_qual_cost.per_tuple;
    cpu_per_tuple = cpu_tuple_cost + qpqual_cost.per_tuple - tid_qual_cost.per_tuple;
    run_cost += cpu_per_tuple * ntuples;

    // Target list evaluation costs
    startup_cost += path->pathtarget->cost.startup;
    run_cost += path->pathtarget->cost.per_tuple * path->rows;

    path->startup_cost = startup_cost;
    path->total_cost = startup_cost + run_cost;
}
```