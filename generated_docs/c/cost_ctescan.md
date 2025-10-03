# cost_ctescan

## Location
[src/backend/optimizer/path/costsize.c:1698-1738](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L1698-L1738)

## Overview
Determines and returns the cost of scanning a CTE (Common Table Expression) RTE, handling both self-referencing and regular CTEs with tuplestore-based access.

## Definition

```c
void
cost_ctescan(Path *path, PlannerInfo *root,
			 RelOptInfo *baserel, ParamPathInfo *param_info)
```
## Detailed Description
This function calculates the execution cost for scanning a Common Table Expression (CTE), which can be either a regular CTE or a recursive CTE's work table. The cost model accounts for:

1. **Tuplestore manipulation**: CTEs are typically materialized in tuplestores, so the function charges one CPU tuple cost per row for accessing the stored tuples
2. **Qualification costs**: Includes costs for applying any WHERE clause restrictions on the CTE
3. **Target list evaluation**: Accounts for computing output columns
4. **Row estimation**: Uses parameterized estimates when available

Important note: The costs of initially evaluating/computing the CTE query itself are handled separately as initplan costs and are NOT included in this function's calculations.

## Parameters / Member Variables
- `*path`: The Path node to store the calculated costs (startup_cost and total_cost fields are set)
- `*root`: PlannerInfo structure containing global planning information and cost parameters
- `*baserel`: RelOptInfo representing the CTE relation being scanned (must have rtekind == RTE_CTE)
- `*param_info`: ParamPathInfo for parameterized paths, or NULL for non-parameterized scans
## Dependencies
- Functions called/Symbols referenced:
  - [get_restriction_qual_cost](../g/get_restriction_qual_cost.md) (calculates cost of applying restriction qualifiers)
  - cpu_tuple_cost (global cost parameter for tuple processing)
- Types referenced:
  - [ParamPathInfo](../P/ParamPathInfo.md) (parameterized path information)
  - Cost (cost calculation type)
  - [QualCost](../Q/QualCost.md) (qualification cost structure)
  - RTE_CTE (enum value for CTE range table entries)
- Called from:
  - [create_ctescan_path](create_ctescan_path.md) (in pathnode.c:2139)
  - [create_worktablescan_path](create_worktablescan_path.md) (in pathnode.c:2218)

## Notes and Other Information
- The function handles both regular CTEs and recursive CTE work tables with the same cost model, as the differences are considered below the threshold of accurate estimation
- Includes an assertion ensuring the relation is a CTE (rtekind == RTE_CTE)
- The cost model assumes tuplestore-based access, charging double cpu_tuple_cost (once for tuplestore manipulation, once for standard tuple processing)
- CTE evaluation costs are excluded here because they're handled as initplan costs in the overall query plan
- Target list costs are applied per output row rather than per scanned tuple, accounting for potential filtering

## Simplified Source
```c
void
cost_ctescan(Path *path, PlannerInfo *root,
            RelOptInfo *baserel, ParamPathInfo *param_info)
{
    Cost startup_cost = 0;
    Cost run_cost = 0;
    QualCost qual_cost;
    Cost cpu_per_tuple;

    // Validate this is a CTE relation
    Assert(baserel->relid > 0);
    Assert(baserel->rtekind == RTE_CTE);

    // Set row estimate (parameterized or base estimate)
    if (param_info)
        path->rows = param_info->ppi_rows;
    else
        path->rows = baserel->rows;

    // Base cost: one CPU tuple cost for tuplestore access
    cpu_per_tuple = cpu_tuple_cost;

    // Add qualification costs (WHERE clauses)
    get_restriction_qual_cost(root, baserel, param_info, &qual_cost);
    startup_cost += qual_cost.startup;
    cpu_per_tuple += cpu_tuple_cost + qual_cost.per_tuple;

    // Total scan cost = cpu cost × number of tuples
    run_cost += cpu_per_tuple * baserel->tuples;

    // Add target list evaluation costs (applied per output row)
    startup_cost += path->pathtarget->cost.startup;
    run_cost += path->pathtarget->cost.per_tuple * path->rows;

    // Set final costs
    path->startup_cost = startup_cost;
    path->total_cost = startup_cost + run_cost;
}
```