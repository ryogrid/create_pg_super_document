# create_seqscan_plan

## Location
[src/backend/optimizer/plan/createplan.c:2917-2954](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L2917-L2954)

## Overview
Creates a sequential scan plan node for scanning a base relation with specified target list and restriction clauses.

## Definition

```c
static SeqScan *
create_seqscan_plan(PlannerInfo *root, Path *best_path,
					List *tlist, List *scan_clauses)
```
## Detailed Description
The  function is responsible for creating a  plan node that represents a sequential scan operation on a base relation. This function is part of PostgreSQL's query planner infrastructure and converts a path representation into an executable plan node. The function performs several important steps:

1. Validates that the target relation is a base relation (not a join or subquery)
2. Orders the qualification clauses for optimal execution
3. Extracts actual boolean expressions from RestrictInfo structures
4. Handles parameterized paths by replacing outer-relation variables with nestloop parameters
5. Creates the final SeqScan node and copies generic path information

The sequential scan is the most basic access method in PostgreSQL, reading every tuple in a relation from beginning to end.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing global planner state and context information
- `*best_path`: The chosen Path representing the sequential scan, containing cost estimates and relation information
- `*tlist`: Target list specifying which columns/expressions should be returned by the scan
- `*scan_clauses`: List of RestrictInfo nodes representing WHERE clause conditions to be applied during the scan
## Dependencies
- Functions called/Symbols referenced:
  - [order_qual_clauses](../o/order_qual_clauses.md)
  - [extract_actual_clauses](../e/extract_actual_clauses.md)
  - [replace_nestloop_params](../r/replace_nestloop_params.md)
  - [make_seqscan](../m/make_seqscan.md)
  - [copy_generic_path_info](copy_generic_path_info.md)
  - RTE_RELATION (enum value)
  - [SeqScan](../S/SeqScan.md) (struct type)
- Called from (representative examples):
  - [create_scan_plan](create_scan_plan.md)

## Notes and Other Information
- This function is static and only used within the createplan.c module
- Includes assertion checks to ensure the path represents a valid base relation
- Handles both regular and parameterized sequential scans
- The function follows PostgreSQL's pattern of separating path optimization from plan creation
- Sequential scans are typically chosen when no suitable indexes are available or when the optimizer estimates that scanning the entire table would be more efficient than index-based access

## Simplified Source

```c
static SeqScan *
create_seqscan_plan(PlannerInfo *root, Path *best_path,
                    List *tlist, List *scan_clauses)
{
    SeqScan *scan_plan;
    Index scan_relid = best_path->parent->relid;

    // Validate that we have a base relation
    Assert(scan_relid > 0);
    Assert(best_path->parent->rtekind == RTE_RELATION);

    // Optimize scan clauses for best execution order
    scan_clauses = order_qual_clauses(root, scan_clauses);

    // Convert RestrictInfo structures to plain expressions
    scan_clauses = extract_actual_clauses(scan_clauses, false);

    // Handle parameterized paths by replacing outer variables with nestloop params
    if (best_path->param_info) {
        scan_clauses = (List *) replace_nestloop_params(root, (Node *) scan_clauses);
    }

    // Create the SeqScan plan node
    scan_plan = make_seqscan(tlist, scan_clauses, scan_relid);

    // Copy standard path information (costs, etc.) to the plan
    copy_generic_path_info(&scan_plan->scan.plan, best_path);

    return scan_plan;
}
```