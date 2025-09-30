# create_valuesscan_plan

## Location
[src/backend/optimizer/plan/createplan.c:3847-3890](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L3847-L3890)

## Overview
Creates a ValuesScan plan node for scanning a VALUES clause base relation with the specified target list and scan clauses.

## Definition

```c
static ValuesScan *
create_valuesscan_plan(PlannerInfo *root, Path *best_path,
					   List *tlist, List *scan_clauses)
```
## Detailed Description
This function creates a ValuesScan plan node for executing a VALUES clause scan. It processes the VALUES lists from the range table entry, handles restriction clauses by sorting them for optimal execution order, and manages nestloop parameter substitution when the path has parameter information. The function extracts the VALUES lists from the corresponding range table entry and ensures proper integration with the rest of the query plan through generic path information copying.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planner state and context information
- `best_path`: Path structure representing the chosen access path for this VALUES scan
- `tlist`: Target list specifying which columns/expressions to return from the scan
- `scan_clauses`: List of restriction clauses (WHERE conditions) to apply during scanning

## Dependencies
- Functions called/Symbols referenced:
  - planner_rt_fetch
  - [order_qual_clauses](../o/order_qual_clauses.md)
  - [extract_actual_clauses](../e/extract_actual_clauses.md)
  - [replace_nestloop_params](../r/replace_nestloop_params.md)
  - [make_valuesscan](../m/make_valuesscan.md)
  - [copy_generic_path_info](copy_generic_path_info.md)
  - [ValuesScan](../V/ValuesScan.md) (return type)
  - RTE_VALUES (constant)
- Called from (representative examples):
  - [create_scan_plan](create_scan_plan.md)

## Notes and Other Information
- The function assumes the scan_relid corresponds to a VALUES base relation (RTE_VALUES)
- Handles nestloop parameter substitution for both scan clauses and VALUES lists when parameterized paths are involved
- The restriction clauses are optimized by sorting them into the best execution order before being processed
- Pseudoconstant clauses are filtered out during clause extraction to improve execution efficiency

## Simplified Source

```c
static ValuesScan *
create_valuesscan_plan(PlannerInfo *root, Path *best_path,
                       List *tlist, List *scan_clauses)
{
    ValuesScan *scan_plan;
    Index scan_relid = best_path->parent->relid;
    RangeTblEntry *rte;
    List *values_lists;

    // Validate that we have a VALUES relation
    Assert(scan_relid > 0);
    rte = planner_rt_fetch(scan_relid, root);
    Assert(rte->rtekind == RTE_VALUES);
    values_lists = rte->values_lists;

    // Optimize scan clauses for best execution order
    scan_clauses = order_qual_clauses(root, scan_clauses);

    // Convert RestrictInfo structures to plain expressions
    scan_clauses = extract_actual_clauses(scan_clauses, false);

    // Handle parameterized paths: replace params in both scan clauses and VALUES lists
    if (best_path->param_info) {
        scan_clauses = (List *) replace_nestloop_params(root, (Node *) scan_clauses);
        values_lists = (List *) replace_nestloop_params(root, (Node *) values_lists);
    }

    // Create the ValuesScan plan node
    scan_plan = make_valuesscan(tlist, scan_clauses, scan_relid, values_lists);

    // Copy standard path information (costs, etc.) to the plan
    copy_generic_path_info(&scan_plan->scan.plan, best_path);

    return scan_plan;
}
```