# make_functionscan

## Location
[src/backend/optimizer/plan/createplan.c:5704-5724](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L5704-L5724)

## Overview
Creates and initializes a FunctionScan plan node, which represents a scan operation on the result set returned by one or more table-valued functions in PostgreSQL's query execution plan.

## Definition
```c
static FunctionScan *
make_functionscan(List *qptlist,
                  List *qpqual,
                  Index scanrelid,
                  List *functions,
                  bool funcordinality)
```

## Detailed Description
The `make_functionscan` function is a factory function that constructs a FunctionScan plan node. This node type is used when the query planner needs to scan the results of table-valued functions (functions that return a set of rows). The function allocates memory for a new FunctionScan node, initializes its base Plan structure with the provided target list and qualification conditions, and sets up the function-specific fields including the list of functions to execute and whether ordinality (row numbering) is requested.

FunctionScan nodes are commonly used for queries involving functions like `generate_series()`, `unnest()`, or user-defined table functions that appear in the FROM clause of a SQL query.

## Parameters / Member Variables
- `qptlist`: The target list (projection list) specifying which columns/expressions to return from the function scan
- `qpqual`: The qualification conditions (WHERE clause predicates) to be applied during the scan
- `scanrelid`: The relation ID assigned to this scan operation for identification purposes
- `functions`: A list of RangeTblFunction nodes representing the functions to be executed and scanned
- `funcordinality`: Boolean flag indicating whether to include row numbers (ordinality) in the output

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to allocate FunctionScan node)
  - [FunctionScan](../F/FunctionScan.md) (node type)
- Called from (representative examples):
  - [create_functionscan_plan](../c/create_functionscan_plan.md)

## Notes and Other Information
- This is a static function within createplan.c, indicating it's an internal helper for plan creation
- The function follows PostgreSQL's pattern of setting lefttree and righttree to NULL for leaf scan nodes
- The funcordinality parameter supports SQL's WITH ORDINALITY clause that adds row numbers to function output
- Multiple functions can be specified in the functions list, allowing for lateral joins between table functions
- Part of PostgreSQL's query planner infrastructure that handles table-valued functions in SQL queries

## Simplified Source

```c
static FunctionScan *make_functionscan(List *qptlist, List *qpqual, Index scanrelid,
                                       List *functions, bool funcordinality) {
    // Create new FunctionScan node
    FunctionScan *node = makeNode(FunctionScan);
    Plan *plan = &node->scan.plan;

    // Set up basic plan fields
    plan->targetlist = qptlist;
    plan->qual = qpqual;
    plan->lefttree = NULL;   // Leaf scan node
    plan->righttree = NULL;  // Leaf scan node

    // Set function scan specific fields
    node->scan.scanrelid = scanrelid;
    node->functions = functions;            // List of table functions
    node->funcordinality = funcordinality;  // Include row numbers if true

    return node;
}
```