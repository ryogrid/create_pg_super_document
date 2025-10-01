# cost_functionscan

## Location
[src/backend/optimizer/path/costsize.c:1531-1591](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L1531-L1591)

## Overview
Determines and returns the cost of scanning a function RTE, calculating costs for accessing results from table-valued functions in the FROM clause.

## Definition

```c
void
cost_functionscan(Path *path, PlannerInfo *root,
				  RelOptInfo *baserel, ParamPathInfo *param_info)
```
## Detailed Description
The `cost_functionscan` function calculates the cost of scanning a table-valued function by evaluating the cost of executing the function expression(s) and adding the overhead of tuple processing and qualification checking. The costing model reflects the current implementation behavior where nodeFunctionscan.c executes functions to completion before returning any rows, caching results in a tuplestore. This means that function evaluation costs are treated as startup costs rather than per-tuple costs. The function accounts for the cost of evaluating function expressions, applying any restriction clauses, and processing the target list, but does not currently account for potential tuplestore spill costs despite acknowledging this as a future refinement.

## Parameters / Member Variables
- `path`: Output parameter where the calculated costs will be stored
- `root`: PlannerInfo structure containing global planner state
- `baserel`: RelOptInfo for the function relation
- `param_info`: ParamPathInfo for parameterized paths, or NULL for non-parameterized paths

## Dependencies
- Functions called/Symbols referenced:
  - planner_rt_fetch
  - [cost_qual_eval_node](cost_qual_eval_node.md)
  - [get_restriction_qual_cost](../g/get_restriction_qual_cost.md)
  - RTE_FUNCTION (constant)
- Called from (representative examples):
  - [create_functionscan_path](create_functionscan_path.md)

## Notes and Other Information
- Only applies to base relations that are functions (RTE_FUNCTION)
- Function evaluation cost is treated as startup cost due to current implementation caching behavior
- Does not currently account for tuplestore spill costs, though this is acknowledged as a potential future improvement
- Row count estimates for functions are often imprecise, affecting cost accuracy
- Target list evaluation costs are applied per output row, not per scanned tuple
- Handles both parameterized and non-parameterized function scans

## Simplified Source

```c
void
cost_functionscan(Path *path, PlannerInfo *root,
                  RelOptInfo *baserel, ParamPathInfo *param_info)
{
    Cost startup_cost = 0;
    Cost run_cost = 0;
    QualCost qual_cost;
    Cost cpu_per_tuple;
    RangeTblEntry *rte;
    QualCost expr_cost;

    // Get the function RTE
    rte = planner_rt_fetch(baserel->relid, root);

    // Set row count from param_info or baserel
    if (param_info)
        path->rows = param_info->ppi_rows;
    else
        path->rows = baserel->rows;

    // Calculate function execution cost
    // Functions execute to completion before returning rows (cached in tuplestore)
    // So function eval cost is startup cost, not per-row cost
    cost_qual_eval_node(&expr_cost, (Node *) rte->functions, root);
    startup_cost += expr_cost.startup + expr_cost.per_tuple;

    // Add restriction qualification costs
    get_restriction_qual_cost(root, baserel, param_info, &qual_cost);
    startup_cost += qual_cost.startup;
    cpu_per_tuple = cpu_tuple_cost + qual_cost.per_tuple;
    run_cost += cpu_per_tuple * baserel->tuples;

    // Target list evaluation costs are per output row
    startup_cost += path->pathtarget->cost.startup;
    run_cost += path->pathtarget->cost.per_tuple * path->rows;

    // Set final costs
    path->startup_cost = startup_cost;
    path->total_cost = startup_cost + run_cost;
}
```