# create_tidrangescan_plan

## Location
[src/backend/optimizer/plan/createplan.c:3637-3701](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L3637-L3701)

## Overview
Creates a TID range scan plan for a base relation that scans a range of tuple identifiers (TIDs) rather than individual TIDs, optimized for range-based TID queries.

## Definition

```c
static TidRangeScan *
create_tidrangescan_plan(PlannerInfo *root, TidRangePath *best_path,
						 List *tlist, List *scan_clauses)
```
## Detailed Description
The  function constructs a TidRangeScan execution plan node for scanning ranges of tuple identifiers. Unlike regular TID scans that target specific individual TIDs, this function handles queries that specify TID ranges, allowing for efficient scanning of consecutive rows in a table.

The function is simpler than  because TID range qualifications use AND semantics rather than OR semantics, making duplicate elimination straightforward. It processes the tidrangequals list to filter out redundant scan clauses and prepares the final scan plan.

Key processing steps include:
- Filtering scan clauses to remove duplicates found in tidrangequals (using simple AND semantics)
- Converting RestrictInfo structures to bare expressions
- Replacing outer relation variables with nestloop parameters when needed
- Creating the final TidRangeScan plan with optimized execution order

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing planner state and context information
- `*best_path`: TidRangePath representing the chosen access path with TID range qualifications
- `*tlist`: Target list specifying which columns to return from the scan
- `*scan_clauses`: List of restriction clauses to apply during scanning
## Dependencies
- Functions called/Symbols referenced:
  - [list_member_ptr](../l/list_member_ptr.md)
  - [order_qual_clauses](../o/order_qual_clauses.md)
  - [extract_actual_clauses](../e/extract_actual_clauses.md)
  - [replace_nestloop_params](../r/replace_nestloop_params.md)
  - [make_tidrangescan](../m/make_tidrangescan.md)
  - [copy_generic_path_info](copy_generic_path_info.md)
- Called from (representative examples):
  - [create_scan_plan](create_scan_plan.md)

## Notes and Other Information
- Only works with base relations (RTE_RELATION), similar to regular TID scans
- Uses AND semantics for tidrangequals, making duplicate elimination simpler than TID scans
- More efficient than individual TID scans when scanning consecutive or nearly consecutive rows
- Supports parameterized plans through nestloop parameter replacement
- Particularly useful for queries with TID range conditions like 
- The scan can efficiently process ranges of TIDs without needing to specify each individual TID

## Simplified Source

```c
static TidRangeScan *
create_tidrangescan_plan(PlannerInfo *root, TidRangePath *best_path,
                         List *tlist, List *scan_clauses)
{
    TidRangeScan *scan_plan;
    Index scan_relid = best_path->path.parent->relid;
    List *tidrangequals = best_path->tidrangequals;

    // Validate that we have a base relation
    Assert(scan_relid > 0);
    Assert(best_path->path.parent->rtekind == RTE_RELATION);

    // Filter scan clauses: remove duplicates found in tidrangequals (AND semantics)
    {
        List *qpqual = NIL;
        ListCell *l;

        foreach(l, scan_clauses) {
            RestrictInfo *rinfo = lfirst_node(RestrictInfo, l);

            if (rinfo->pseudoconstant)
                continue;  // Drop pseudoconstants
            if (list_member_ptr(tidrangequals, rinfo))
                continue;  // Remove duplicates
            qpqual = lappend(qpqual, rinfo);
        }
        scan_clauses = qpqual;
    }

    // Optimize scan clauses for best execution order
    scan_clauses = order_qual_clauses(root, scan_clauses);

    // Convert RestrictInfo structures to plain expressions
    tidrangequals = extract_actual_clauses(tidrangequals, false);
    scan_clauses = extract_actual_clauses(scan_clauses, false);

    // Handle parameterized paths by replacing outer variables with nestloop params
    if (best_path->path.param_info) {
        tidrangequals = (List *) replace_nestloop_params(root, (Node *) tidrangequals);
        scan_clauses = (List *) replace_nestloop_params(root, (Node *) scan_clauses);
    }

    // Create the TidRangeScan plan node
    scan_plan = make_tidrangescan(tlist, scan_clauses, scan_relid, tidrangequals);

    // Copy standard path information (costs, etc.) to the plan
    copy_generic_path_info(&scan_plan->scan.plan, &best_path->path);

    return scan_plan;
}
```