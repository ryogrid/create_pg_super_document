# create_tablefuncscan_path

## Location
src/backend/optimizer/util/pathnode.c: 2072 - 2097

## Overview
Creates a Path node corresponding to a sequential scan of a table function, which represents accessing results from table functions like XMLTABLE, JSON_TABLE, or other structured data transformation functions in PostgreSQL's query planner.

## Definition
```c
Path *create_tablefuncscan_path(PlannerInfo *root, RelOptInfo *rel,
                               Relids required_outer)
```

## Detailed Description
The create_tablefuncscan_path function constructs a basic Path node that represents scanning a table function's output. Table functions are special functions that transform structured data (like XML or JSON) into relational format. Unlike regular functions, table functions have more complex internal processing but are still represented with a simple Path node in the planner.

Key behaviors include:
- Creates a basic Path node (not a specialized subtype) for table function scanning
- Always produces unordered results (pathkeys = NIL) since table functions don't guarantee ordering
- Sets parallel safety based on the relation's consider_parallel flag
- Does not support parallel workers (parallel_workers = 0)
- Uses the relation's target list as the output specification
- Delegates cost calculation to cost_tablefuncscan for accurate estimates

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planning information and context
- `rel`: RelOptInfo structure representing the table function relation being scanned
- `required_outer`: Relids indicating which outer relations are required for parameter passing

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create Path node)
  - get_baserel_parampathinfo (to get parameter information)
  - [cost_tablefuncscan](cost_tablefuncscan.md) (to calculate execution costs)

- Called from (representative examples):
  - [set_tablefunc_pathlist](../s/set_tablefunc_pathlist.md) (in allpaths.c:2848)

## Notes and Other Information
- Returns a basic Path node rather than a specialized path type, similar to regular function scans
- Table function scans always produce unordered results, unlike some other scan types
- Not parallel-aware and does not use parallel workers due to the complex nature of table function processing
- The parallel_safe property depends on the relation's consider_parallel setting
- Commonly used for XMLTABLE, JSON_TABLE, and other structured data transformation operations
- Cost calculation considers the complexity of data transformation and result set generation
- Table functions differ from regular functions in that they process structured input to produce tabular output