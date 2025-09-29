# find_window_functions

## Location
[src/backend/optimizer/util/clauses.c:227-238](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L227-L238)

## Overview
Locates all WindowFunc nodes in an expression tree and organizes them by their window reference ID (winref) numbers.

## Definition

```c
WindowFuncLists *
find_window_functions(Node *clause, Index maxWinRef)
```
## Detailed Description
This function performs a comprehensive search through an expression tree to locate all window function nodes (WindowFunc) and organize them into lists grouped by their window reference IDs. It allocates a WindowFuncLists structure that contains an array of lists, where each list corresponds to a specific winref ID and contains all window functions that reference that particular window specification. The function requires the caller to provide an upper bound on the expected winref IDs to properly size the internal data structures.

## Parameters / Member Variables
- : A Node pointer representing the expression tree to search for window functions
- : An Index specifying the maximum window reference ID expected in the tree, used to size the internal arrays

## Dependencies
- Functions called/Symbols referenced:
  - [WindowFuncLists](../W/WindowFuncLists.md)
  - [find_window_functions_walker](find_window_functions_walker.md)
- Called from (representative examples):
  - [grouping_planner](../g/grouping_planner.md)
  - [WindowFuncLists](../W/WindowFuncLists.md)

## Notes and Other Information
- The function allocates memory for the WindowFuncLists structure and its internal arrays
- Uses palloc0 to initialize the windowFuncs array to NULL pointers
- The returned structure contains both the organized lists and metadata (numWindowFuncs, maxWinRef)
- The actual traversal and collection work is delegated to 
- This is part of the window function processing infrastructure in the PostgreSQL query planner
- Essential for organizing window functions before creating window execution plans

## Simplified Source

```c
WindowFuncLists *find_window_functions(Node *clause, Index maxWinRef)
{
    WindowFuncLists *lists = palloc(sizeof(WindowFuncLists));

    // Initialize the structure
    lists->numWindowFuncs = 0;
    lists->maxWinRef = maxWinRef;
    lists->windowFuncs = (List **) palloc0((maxWinRef + 1) * sizeof(List *));

    // Walk the expression tree to find and organize window functions
    (void) find_window_functions_walker(clause, lists);

    return lists;
}
```