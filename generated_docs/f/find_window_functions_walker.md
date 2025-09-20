# find_window_functions_walker

## Location
[src/backend/optimizer/util/clauses.c:239-288](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L239-L288)

## Overview
A recursive tree walker function that collects WindowFunc nodes from an expression tree and organizes them by window reference ID into the provided WindowFuncLists structure.

## Definition

```c
static bool
find_window_functions_walker(Node *node, WindowFuncLists *lists)
```
## Detailed Description
This static function serves as the core implementation for collecting and organizing window functions within expression trees. It traverses the node tree recursively using the expression_tree_walker framework, specifically looking for WindowFunc nodes. When a WindowFunc is found, the function validates that its winref ID is within the expected range, checks for duplicates to avoid repeated computation, and adds the function to the appropriate list in the WindowFuncLists structure. The function assumes that the parser has already validated that window functions don't contain nested window functions in their arguments or filter clauses, so it doesn't recurse into those sub-expressions.

## Parameters / Member Variables
- : A Node pointer representing the current node being examined in the expression tree
- : A WindowFuncLists pointer to the structure where found window functions are organized by winref ID

## Dependencies
- Functions called/Symbols referenced:
  - WindowFunc
  - WindowFuncLists
  - [list_member](../l/list_member.md)
  - SubLink
  - expression_tree_walker
  - [find_window_functions_walker](find_window_functions_walker.md) (recursive call)
- Called from (representative examples):
  - [find_window_functions](find_window_functions.md)
  - max_parallel_hazard_context
  - [find_window_functions_walker](find_window_functions_walker.md) (recursive)

## Notes and Other Information
- This is a static function, accessible only within the same compilation unit
- Performs range validation on winref IDs and throws an error if out of bounds
- Uses list_member to eliminate duplicate window functions in the same winref group
- Assumes parser validation prevents nested window functions in arguments/filter clauses
- Returns false to continue traversal, unlike some walker functions that return true to short-circuit
- Asserts that SubLink nodes should not be present at this stage
- Essential component of the window function organization system in PostgreSQL's query planner