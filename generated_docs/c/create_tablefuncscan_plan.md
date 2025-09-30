# create_tablefuncscan_plan

## Location
[src/backend/optimizer/plan/createplan.c:3804-3846](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L3804-L3846)

## Overview
Creates a table function scan plan for scanning table functions such as XMLTABLE, JSON_TABLE, or other structured data parsing functions that generate table-like output.

## Definition

```c
static TableFuncScan *
create_tablefuncscan_plan(PlannerInfo *root, Path *best_path,
						  List *tlist, List *scan_clauses)
```
## Detailed Description
The  function constructs a TableFuncScan execution plan node for scanning the output of table functions. Table functions are specialized functions that parse structured data (like XML or JSON) and present it in a tabular format. Unlike regular function scans, table functions have more complex internal structure with column definitions, namespaces, and parsing instructions.

The function processes the TableFunc structure from the range table entry, which contains all the metadata needed for parsing and column extraction. It handles parameter replacement for both scan clauses and the table function expressions themselves.

Key processing steps include:
- Extracting the TableFunc structure from the range table entry
- Processing and ordering scan restriction clauses
- Replacing outer relation variables with nestloop parameters in both scan clauses and table function expressions
- Creating the final TableFuncScan plan

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and context information
- : Path representing the chosen access path for the table function scan
- : Target list specifying which columns to return from the table function scan
- : List of restriction clauses to apply to the table function results

## Dependencies
- Functions called/Symbols referenced:
  - planner_rt_fetch
  - [order_qual_clauses](../o/order_qual_clauses.md)
  - [extract_actual_clauses](../e/extract_actual_clauses.md)
  - [replace_nestloop_params](../r/replace_nestloop_params.md)
  - [make_tablefuncscan](../m/make_tablefuncscan.md)
  - [copy_generic_path_info](copy_generic_path_info.md)
- Called from (representative examples):
  - [create_scan_plan](create_scan_plan.md)

## Notes and Other Information
- Only works with table function relations (RTE_TABLEFUNC), not regular functions or tables
- Handles complex TableFunc structures containing column definitions and parsing instructions
- Table function expressions can contain nestloop parameters requiring replacement
- Used for XMLTABLE, JSON_TABLE, and similar structured data parsing functions
- More specialized than regular function scans due to the structured nature of table functions
- Supports lateral references and parameterized table functions
- Essential for SQL/XML and SQL/JSON standard compliance

## Simplified Source

```c
static TableFuncScan *
create_tablefuncscan_plan(PlannerInfo *root, Path *best_path,
                          List *tlist, List *scan_clauses)
{
    TableFuncScan *scan_plan;
    Index scan_relid = best_path->parent->relid;
    RangeTblEntry *rte;
    TableFunc *tablefunc;

    // Validate that we have a table function relation
    Assert(scan_relid > 0);
    rte = planner_rt_fetch(scan_relid, root);
    Assert(rte->rtekind == RTE_TABLEFUNC);
    tablefunc = rte->tablefunc;

    // Optimize scan clauses for best execution order
    scan_clauses = order_qual_clauses(root, scan_clauses);

    // Convert RestrictInfo structures to plain expressions
    scan_clauses = extract_actual_clauses(scan_clauses, false);

    // Handle parameterized paths: replace params in both scan clauses and table function
    if (best_path->param_info) {
        scan_clauses = (List *) replace_nestloop_params(root, (Node *) scan_clauses);
        tablefunc = (TableFunc *) replace_nestloop_params(root, (Node *) tablefunc);
    }

    // Create the TableFuncScan plan node
    scan_plan = make_tablefuncscan(tlist, scan_clauses, scan_relid, tablefunc);

    // Copy standard path information (costs, etc.) to the plan
    copy_generic_path_info(&scan_plan->scan.plan, best_path);

    return scan_plan;
}
```