# cost_tablefuncscan

## Location
[src/backend/optimizer/path/costsize.c:1592-1647](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L1592-L1647)

## Overview
Determines and returns the cost of scanning a table function, calculating costs for accessing results from table functions like XMLTABLE, JSON_TABLE, etc.

## Definition

```c
void
cost_tablefuncscan(Path *path, PlannerInfo *root,
				   RelOptInfo *baserel, ParamPathInfo *param_info)
```
## Detailed Description
The `cost_tablefuncscan` function calculates the cost of scanning a table function (such as XMLTABLE, JSON_TABLE, or other table-generating functions) by evaluating the cost of executing the table function expression and adding the overhead of tuple processing and qualification checking. Similar to regular function scans, the table function evaluation cost is treated as startup cost, reflecting that the function is typically executed to completion before returning results. The function accounts for the cost of evaluating table function expressions, applying any restriction clauses, and processing the target list. Like `cost_functionscan`, it does not currently account for tuplestore spill costs, acknowledging that row count estimates for table functions are often imprecise.

## Parameters / Member Variables
- `path`: Output parameter where the calculated costs will be stored
- `root`: PlannerInfo structure containing global planner state  
- `baserel`: RelOptInfo for the table function relation
- `param_info`: ParamPathInfo for parameterized paths, or NULL for non-parameterized paths

## Dependencies
- Functions called/Symbols referenced:
  - planner_rt_fetch
  - [cost_qual_eval_node](cost_qual_eval_node.md)
  - [get_restriction_qual_cost](../g/get_restriction_qual_cost.md)
  - RTE_TABLEFUNC (constant)
- Called from (representative examples):
  - [create_tablefuncscan_path](create_tablefuncscan_path.md)

## Notes and Other Information
- Only applies to base relations that are table functions (RTE_TABLEFUNC)
- Table function evaluation cost is treated as startup cost due to typical execution patterns
- Does not currently account for tuplestore spill costs, similar to regular function scans
- Row count estimates for table functions are often imprecise, affecting cost accuracy
- Target list evaluation costs are applied per output row, not per scanned tuple
- Handles both parameterized and non-parameterized table function scans
- Used for table functions like XMLTABLE, JSON_TABLE, and other SQL standard table functions

## Simplified Source

```c
void cost_tablefuncscan(Path *path, PlannerInfo *root,
                        RelOptInfo *baserel, ParamPathInfo *param_info)
{
    Cost startup_cost = 0;
    Cost run_cost = 0;
    QualCost qpqual_cost;
    Cost cpu_per_tuple;
    RangeTblEntry *rte;
    QualCost exprcost;

    // Verify this is a table function relation
    rte = planner_rt_fetch(baserel->relid, root);

    // Set row estimate based on parameterization
    if (param_info)
        path->rows = param_info->ppi_rows;
    else
        path->rows = baserel->rows;

    // Calculate cost of executing the table function expression
    cost_qual_eval_node(&exprcost, (Node *) rte->tablefunc, root);
    startup_cost += exprcost.startup + exprcost.per_tuple;

    // Add restriction qualification costs
    get_restriction_qual_cost(root, baserel, param_info, &qpqual_cost);
    startup_cost += qpqual_cost.startup;
    cpu_per_tuple = cpu_tuple_cost + qpqual_cost.per_tuple;
    run_cost += cpu_per_tuple * baserel->tuples;

    // Add target list evaluation costs (per output row)
    startup_cost += path->pathtarget->cost.startup;
    run_cost += path->pathtarget->cost.per_tuple * path->rows;

    // Store final costs
    path->startup_cost = startup_cost;
    path->total_cost = startup_cost + run_cost;
}
```