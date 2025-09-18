# make_path_initial_array

## Location
src/backend/rewrite/rewriteSearchCycle.c: 159 - 179

## Overview
Wraps a RowExpr in an ArrayExpr to create the initial array structure for CTE SEARCH depth-first or CYCLE path tracking.

## Definition
```c
static Expr *make_path_initial_array(RowExpr *rowexpr)
```

## Detailed Description
This static helper function takes a RowExpr and wraps it in an ArrayExpr to create an array containing a single row element. This is used in the CTE rewriting process to initialize the path tracking array for both SEARCH depth-first operations and CYCLE detection. The resulting ArrayExpr has a record array type (RECORDARRAYOID) with record elements (RECORDOID), making it suitable for storing sequences of row values that represent the traversal path in recursive CTE queries.

## Parameters / Member Variables
- `rowexpr`: Pointer to a RowExpr that will be wrapped as the first element of the array

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create ArrayExpr)
  - list_make1 (to create single-element list)
- Called from:
  - [rewriteSearchAndCycle](../r/rewriteSearchAndCycle.md) (at lines 329 and 344)

## Notes and Other Information
- This is a static helper function only used within rewriteSearchCycle.c
- The function creates an array with exactly one element (the provided RowExpr)
- Used to initialize both SEARCH depth-first path arrays and CYCLE detection path arrays
- The location field is set to -1 indicating no specific source location for the constructed node
- The resulting array serves as the starting point for path accumulation in recursive CTE processing