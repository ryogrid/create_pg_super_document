# cost_subqueryscan

## Location
[src/backend/optimizer/path/costsize.c:1451-1530](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L1451-L1530)

## Overview
Determines and returns the cost of scanning a subquery RTE, calculating costs for accessing results from a subquery as if it were a base relation.

## Definition

```c
void
cost_subqueryscan(SubqueryScanPath *path, PlannerInfo *root,
				  RelOptInfo *baserel, ParamPathInfo *param_info,
				  bool trivial_pathtarget)
```
## Detailed Description
The `cost_subqueryscan` function calculates the cost of scanning a subquery by building upon the cost of the underlying subplan and adding the overhead of any additional restriction clauses and target list evaluation. The function performs an important optimization: when there are no relevant restriction clauses and the pathtarget is trivial, it recognizes that the SubqueryScan node will likely be optimized away during plan creation, so it returns early without adding overhead costs. For non-trivial cases, it computes row estimates by applying selectivity of restriction clauses to the subpath's row estimate, then adds CPU costs for tuple processing and target list evaluation on top of the subplan's costs.

## Parameters / Member Variables
- `path`: SubqueryScanPath where the calculated costs will be stored
- `root`: PlannerInfo structure containing global planner state
- `baserel`: RelOptInfo for the subquery relation
- `param_info`: ParamPathInfo for parameterized paths, or NULL for non-parameterized paths
- `trivial_pathtarget`: Boolean indicating whether the pathtarget is expected to be trivial

## Dependencies
- Functions called/Symbols referenced:
  - [clamp_row_est](clamp_row_est.md)
  - [clauselist_selectivity](clauselist_selectivity.md)
  - [list_concat_copy](../l/list_concat_copy.md)
  - [get_restriction_qual_cost](../g/get_restriction_qual_cost.md)
  - JOIN_INNER (constant)
- Called from (representative examples):
  - [create_subqueryscan_path](create_subqueryscan_path.md)

## Notes and Other Information
- Only applies to base relations that are subqueries (RTE_SUBQUERY)
- Row count estimation combines subpath rows with selectivity of restriction clauses
- Includes optimization for trivial cases where the SubqueryScan node may be eliminated
- Handles both parameterized and non-parameterized paths appropriately
- Target list evaluation costs are applied per output row, not per scanned tuple
- The function accounts for potential discrepancies between cost estimates and actual plan structure in edge cases

## Simplified Source

```c
void cost_subqueryscan(SubqueryScanPath *path, PlannerInfo *root,
                      RelOptInfo *baserel, ParamPathInfo *param_info,
                      bool trivial_pathtarget) {
    Cost startup_cost;
    Cost run_cost;
    List *qpquals;
    QualCost qpqual_cost;
    Cost cpu_per_tuple;

    Assert(baserel->relid > 0);
    Assert(baserel->rtekind == RTE_SUBQUERY);

    // Combine restriction clauses for parameterized and non-parameterized paths
    if (param_info)
        qpquals = list_concat_copy(param_info->ppi_clauses, baserel->baserestrictinfo);
    else
        qpquals = baserel->baserestrictinfo;

    // Calculate row estimate: subpath rows * restriction selectivity
    path->path.rows = clamp_row_est(path->subpath->rows *
                                   clauselist_selectivity(root, qpquals, 0, JOIN_INNER, NULL));

    // Start with subpath costs
    path->path.startup_cost = path->subpath->startup_cost;
    path->path.total_cost = path->subpath->total_cost;

    // Optimization: if no restrictions and trivial target, SubqueryScan may be eliminated
    if (qpquals == NIL && trivial_pathtarget)
        return;

    // Add costs for restriction clause evaluation
    get_restriction_qual_cost(root, baserel, param_info, &qpqual_cost);
    startup_cost = qpqual_cost.startup;
    cpu_per_tuple = cpu_tuple_cost + qpqual_cost.per_tuple;
    run_cost = cpu_per_tuple * path->subpath->rows;

    // Add target list evaluation costs (paid per output row)
    startup_cost += path->path.pathtarget->cost.startup;
    run_cost += path->path.pathtarget->cost.per_tuple * path->path.rows;

    // Apply additional costs to the path
    path->path.startup_cost += startup_cost;
    path->path.total_cost += startup_cost + run_cost;
}
```