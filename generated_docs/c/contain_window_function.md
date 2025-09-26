# contain_window_function

## Location
[src/backend/optimizer/util/clauses.c:214-226](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L214-L226)

## Overview
Recursively searches for window function nodes (WindowFunc) within a clause and returns true if any window functions are found.

## Definition

```c
bool
contain_window_function(Node *clause)
```
## Detailed Description
This function provides a wrapper to detect the presence of window functions within a given expression clause. Unlike aggregate functions that have level fields for handling nested scopes, window functions are hard-wired to be associated with the current query level, which simplifies the detection logic. The function delegates to  from rewriteManip.c, which performs the actual recursive traversal to identify WindowFunc nodes in the expression tree.

## Parameters / Member Variables
- : A Node pointer representing the expression clause to be examined for window functions

## Dependencies
- Functions called/Symbols referenced:
  - [contain_windowfuncs](contain_windowfuncs.md)
  - [WindowFuncLists](../W/WindowFuncLists.md)
- Called from (representative examples):
  - [get_eclass_for_sort_expr](../g/get_eclass_for_sort_expr.md)
  - [WindowFuncLists](../W/WindowFuncLists.md)

## Notes and Other Information
- Window functions don't have level fields like aggregate functions, simplifying the detection process
- The function is essentially a wrapper around the existing  functionality
- Window functions are always associated with the current query level
- Returns a boolean value indicating window function presence
- Part of the window-function clause manipulation utilities in the PostgreSQL optimizer
- The implementation is simpler than aggregate function detection due to the absence of level complexity