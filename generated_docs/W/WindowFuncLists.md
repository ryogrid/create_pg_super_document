# WindowFuncLists

## Location
[src/include/optimizer/clauses.h:24-58](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/optimizer/clauses.h#L24-L58)

## Overview
WindowFuncLists is a data structure used by PostgreSQL's query planner to organize and track window functions found in an expression tree, grouping them by their window reference ID (winref) for efficient processing during query optimization.

## Definition
```c
typedef struct
{
    int         numWindowFuncs; /* total number of WindowFuncs found */
    Index       maxWinRef;      /* windowFuncs[] is indexed 0 .. maxWinRef */
    List      **windowFuncs;    /* lists of WindowFuncs for each winref */
} WindowFuncLists;
```

## Detailed Description
The WindowFuncLists structure serves as an organizational container for window functions encountered during query parsing and planning. Window functions in SQL (such as ROW_NUMBER(), RANK(), SUM() OVER(), etc.) are complex expressions that require special handling during query optimization.

This structure provides an efficient way to:
- Count the total number of window functions in an expression tree
- Group window functions by their window reference ID (winref)
- Eliminate duplicate window function references to avoid repeated computation
- Provide indexed access to window function groups for the planner

The structure is populated by the `find_window_functions()` function, which walks an expression tree and collects all WindowFunc nodes, organizing them into separate lists based on their winref values. Each winref corresponds to a specific window specification (OVER clause) in the original query.

## Parameters / Member Variables
- `numWindowFuncs`: Total count of unique WindowFunc nodes found in the expression tree, used for planning decisions and resource allocation
- `maxWinRef`: Upper bound for the winref indices, determines the size of the windowFuncs array allocation
- `windowFuncs`: Array of List pointers, where each index corresponds to a winref ID and contains a list of WindowFunc nodes sharing that window specification

## Dependencies
- Functions called/Symbols referenced:
  - find_window_functions (primary constructor function)
  - find_window_functions_walker (internal tree walker)
  - contain_window_function (detection utility)
  - WindowFunc (the node type being organized)
  - List (PostgreSQL's generic list structure)
  - Index (PostgreSQL's index type)

- Called from (representative examples):
  - standard_qp_extra (in planner.c)
  - grouping_planner (in planner.c) 
  - create_window_paths (window path generation)
  - create_one_window_path (individual window path creation)
  - optimize_window_clauses (window optimization)
  - select_active_windows (window selection logic)

## Notes and Other Information
- The structure is dynamically allocated using palloc() and the windowFuncs array is zero-initialized with palloc0()
- Window functions with the same winref share the same window specification (OVER clause) and can potentially be computed together for efficiency
- The walker function eliminates duplicates by checking list membership before adding WindowFunc nodes
- This organization is crucial for PostgreSQL's window function optimization, allowing the planner to identify opportunities for shared computation
- The structure is primarily used during the query planning phase and is not needed during execution
- Error checking ensures that winref values do not exceed the declared maxWinRef bound
- The structure supports PostgreSQL's sophisticated window function processing, including frame specifications, partitioning, and ordering