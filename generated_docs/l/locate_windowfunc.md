# locate_windowfunc

## Location
src/backend/rewrite/rewriteManip.c: 254 - 272

## Overview
Finds the parse location of any window function call at the current query level, primarily used for error reporting purposes.

## Definition
```c
int locate_windowfunc(Node *node)
```

## Detailed Description
This function traverses an expression tree to find the first window function call and returns its parse location within the original SQL text. It uses a context structure to track the location as it walks the tree using the locate_windowfunc_walker helper function. The function is specifically designed to find window functions at the current query level only, not descending into subqueries. It returns -1 if no window function is found or if all found window functions have unknown parse locations. This function is primarily used for generating meaningful error messages that can point users to the specific location in their SQL where a window function appears.

## Parameters / Member Variables
- `node`: The root node of the expression tree to search for window functions

## Dependencies
- Functions called/Symbols referenced:
  - [locate_windowfunc_context](locate_windowfunc_context.md) (context structure)
  - query_or_expression_tree_walker (tree traversal function)
  - [locate_windowfunc_walker](locate_windowfunc_walker.md) (helper walker function)
- Called from (representative examples):
  - [transformWindowFuncCall](../t/transformWindowFuncCall.md)
  - [checkTargetlistEntrySQL92](../c/checkTargetlistEntrySQL92.md)

## Notes and Other Information
- Returns -1 if no window function is found or if parse locations are unknown
- Designed specifically for error reporting, so performance optimization is secondary
- The function comment notes that merging this with contain_windowfuncs would complicate that function's simpler API
- Uses query_or_expression_tree_walker to handle both Query nodes and bare expression trees
- Only operates on the current query level, not descending into subqueries
- Part of the query rewrite manipulation infrastructure in PostgreSQL