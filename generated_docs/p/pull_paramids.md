# pull_paramids

## Location
[src/backend/optimizer/util/clauses.c:5418-5427](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L5418-L5427)

## Overview
Extracts parameter IDs from an expression tree and returns them as a Bitmapset containing the paramids of all Params found in the given expression.

## Definition
```c
Bitmapset *pull_paramids(Expr *expr)
```

## Detailed Description
The `pull_paramids` function is a utility function in PostgreSQL's query optimizer that analyzes an expression tree to identify all parameter references within it. It returns a Bitmapset data structure that contains the parameter IDs (paramids) of all Param nodes found in the expression. This function is essential for query planning as it helps the optimizer understand which parameters are referenced by a given expression, which is crucial for plan caching, parameter binding, and optimization decisions.

The function works by delegating the actual tree traversal to `pull_paramids_walker`, which implements the recursive logic to walk through the expression tree and collect parameter IDs.

## Parameters / Member Variables
- `expr`: A pointer to the expression tree (Expr type) from which to extract parameter IDs. This can be any SQL expression that might contain parameter references.

## Dependencies
- Functions called/Symbols referenced:
  - [pull_paramids_walker](pull_paramids_walker.md)
- Called from (representative examples):
  - [create_memoize_plan](../c/create_memoize_plan.md)
  - [WindowFuncLists](../W/WindowFuncLists.md)

## Notes and Other Information
- Returns a Bitmapset containing the paramids, or NULL if no parameters are found
- The function is implemented as a simple wrapper around `pull_paramids_walker` for convenience
- Located in src/backend/optimizer/util/clauses.c at lines 5418-5427
- The returned Bitmapset should be managed appropriately by the caller (freed when no longer needed)
- This function is commonly used in query optimization phases where understanding parameter dependencies is critical