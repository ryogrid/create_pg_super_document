# make_path_cat_expr

## Location
[src/backend/rewrite/rewriteSearchCycle.c:180-202](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteSearchCycle.c#L180-L202)

## Overview
Creates an array concatenation expression that appends a new row to an existing path array for CTE SEARCH depth-first or CYCLE path tracking.

## Definition
```c
static Expr *make_path_cat_expr(RowExpr *rowexpr, AttrNumber path_varattno)
```

## Detailed Description
This static function constructs a FuncExpr that represents an array concatenation operation of the form `cpa || ARRAY[ROW(cols)]`, where `cpa` is an existing path array variable and the new row expression is appended to it. This is used in the recursive part of CTE rewriting to accumulate the traversal path by concatenating the current row values to the existing path array. The function creates both the array wrapper for the new row and the concatenation function call that combines the existing path with the new element.

## Parameters / Member Variables
- `rowexpr`: Pointer to a RowExpr representing the current row values to append to the path
- `path_varattno`: Attribute number of the existing path array variable to concatenate with

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create ArrayExpr)
  - list_make1 (to create single-element list)
  - makeFuncExpr (to create function call expression)
  - makeVar (to create variable reference for existing path)
  - list_make2 (to create two-element argument list)
- Called from:
  - [rewriteSearchAndCycle](../r/rewriteSearchAndCycle.md) (at lines 535 and 581)

## Notes and Other Information
- This is a static helper function only used within rewriteSearchCycle.c
- Creates an explicit function call to F_ARRAY_CAT (array concatenation function)
- The coercion type is set to COERCE_EXPLICIT_CALL indicating an explicit function call
- Used in both SEARCH depth-first and CYCLE detection scenarios to build up the path array
- The path_varattno parameter specifies which column in the CTE contains the path array to extend
- The location field is set to -1 indicating no specific source location for the constructed nodes