# create_functionscan_path

## Location
src/backend/optimizer/util/pathnode.c: 2046 - 2071

## Overview
Creates a Path node corresponding to a sequential scan of a function, which represents accessing the results returned by a table-valued function in PostgreSQL's query planner.

## Definition
```c
Path *create_functionscan_path(PlannerInfo *root, RelOptInfo *rel,
                              List *pathkeys, Relids required_outer)
```

## Detailed Description
The create_functionscan_path function constructs a basic Path node that represents scanning a function's output. This is used when a function appears in the FROM clause (function in FROM clause or LATERAL function) and needs to be treated as a data source. Unlike more complex path types, this creates a simple Path node since function scans are relatively straightforward operations.

Key behaviors include:
- Creates a basic Path node (not a specialized subtype) for function scanning
- Sets parallel safety based on the relation's consider_parallel flag
- Does not support parallel workers (parallel_workers = 0)
- Uses the relation's target list as the output specification
- Delegates cost calculation to cost_functionscan for accurate estimates

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planning information and context
- `rel`: RelOptInfo structure representing the function relation being scanned
- `pathkeys`: List specifying the desired output ordering for the scan results
- `required_outer`: Relids indicating which outer relations are required for parameter passing

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create Path node)
  - get_baserel_parampathinfo (to get parameter information)
  - cost_functionscan (to calculate execution costs)

- Called from (representative examples):
  - set_function_pathlist (in allpaths.c:2807)

## Notes and Other Information
- Returns a basic Path node rather than a specialized path type, indicating function scans are treated as simple operations
- Function scans are not parallel-aware and do not use parallel workers
- The parallel_safe property depends on the relation's consider_parallel setting, which is determined by function properties
- Commonly used for table-valued functions, set-returning functions, and LATERAL function references
- Cost calculation is delegated to cost_functionscan which considers function execution complexity and result set size