# create_namedtuplestorescan_plan

## Location
[src/backend/optimizer/plan/createplan.c:3986-4024](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L3986-L4024)

## Overview
Creates a NamedTuplestoreScan plan node for scanning a named tuplestore relation with the specified target list and scan clauses.

## Definition
```c
static NamedTuplestoreScan *
create_namedtuplestorescan_plan(PlannerInfo *root, Path *best_path,
                                List *tlist, List *scan_clauses)
```

## Detailed Description
This function creates a NamedTuplestoreScan plan node for executing scans on ephemeral named relations (ENRs) that are backed by tuplestores. These relations are typically used for temporary data storage during query execution, such as transition tables in triggers or materialized subquery results. The function processes restriction clauses, handles nestloop parameter substitution, and uses the ENR name from the range table entry to identify the specific tuplestore to scan.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planner state and context information
- `best_path`: Path structure representing the chosen access path for this named tuplestore scan
- `tlist`: Target list specifying which columns/expressions to return from the scan
- `scan_clauses`: List of restriction clauses (WHERE conditions) to apply during scanning

## Dependencies
- Functions called/Symbols referenced:
  - planner_rt_fetch
  - [order_qual_clauses](../o/order_qual_clauses.md)
  - [extract_actual_clauses](../e/extract_actual_clauses.md)
  - [replace_nestloop_params](../r/replace_nestloop_params.md)
  - [make_namedtuplestorescan](../m/make_namedtuplestorescan.md)
  - [copy_generic_path_info](copy_generic_path_info.md)
  - [NamedTuplestoreScan](../N/NamedTuplestoreScan.md) (return type)
  - RTE_NAMEDTUPLESTORE (constant)
- Called from (representative examples):
  - [create_scan_plan](create_scan_plan.md)

## Notes and Other Information
- The function assumes the scan_relid corresponds to a named tuplestore relation (RTE_NAMEDTUPLESTORE)
- Uses the ENR name (enrname) from the range table entry to identify the specific tuplestore during execution
- Named tuplestores are ephemeral relations that exist only during query execution and are commonly used for transition tables in triggers
- The scan clauses are optimized by sorting them into the best execution order before processing
- Supports nestloop parameter substitution when the path involves parameterized access patterns

## Simplified Source

```c
static NamedTuplestoreScan *
create_namedtuplestorescan_plan(PlannerInfo *root, Path *best_path,
                                List *tlist, List *scan_clauses)
{
    NamedTuplestoreScan *scan_plan;
    Index scan_relid = best_path->parent->relid;
    RangeTblEntry *rte;

    // Validate that we have a valid relation ID and fetch the range table entry
    Assert(scan_relid > 0);
    rte = planner_rt_fetch(scan_relid, root);
    Assert(rte->rtekind == RTE_NAMEDTUPLESTORE);

    // Optimize scan clauses for best execution order
    scan_clauses = order_qual_clauses(root, scan_clauses);

    // Convert RestrictInfo structures to plain expressions
    scan_clauses = extract_actual_clauses(scan_clauses, false);

    // Handle parameterized paths by replacing outer variables with nestloop params
    if (best_path->param_info) {
        scan_clauses = (List *) replace_nestloop_params(root, (Node *) scan_clauses);
    }

    // Create the actual NamedTuplestoreScan plan node
    scan_plan = make_namedtuplestorescan(tlist, scan_clauses, scan_relid, rte->enrname);

    // Copy standard path information (costs, etc.) to the plan
    copy_generic_path_info(&scan_plan->scan.plan, best_path);

    return scan_plan;
}
```